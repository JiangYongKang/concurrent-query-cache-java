# concurrent-query-cache-java

面向高并发读取的本地查询加速层：在 Spring Boot Web 服务内提供
**并发合并 + TTL 缓存 + 并发上限/背压 + 可开关的降级旧值 + 后台数据刷新驱动失效**，
使重复请求、慢依赖与并发压力下的响应时间与状态变化保持可预测。

## 架构

```
请求 → QueryFacade
        ├─ 1. QueryCacheStore.getFresh   命中未过期缓存 → CACHE
        ├─ 2. RequestCoalescer.coalesce  同键并发合并，只放行一个计算
        │     ├─ 等待者容量闸门（超出 → WAIT_OVERLOADED）+ 等待超时（WAIT_TIMEOUT）
        │     └─ 双重检查缓存（等待期间可能已被同键请求填充）
        ├─ 3. 发起方捕获数据世代(generation)
        ├─ 4. ConcurrencyLimiter.execute 信号量限流 + 排队背压 + 计算超时
        │     └─ SlowDependency.compute  可本地模拟的慢依赖（延迟/故障/阻塞闸门可注入）
        ├─ 5. commitIfCurrent：世代已被刷新推进则丢弃本次结果并在新世代重算
        └─ 6. 失败时：降级开关打开且有同世代旧值 → STALE_DEGRADED，否则抛出可区分原因的异常

后台刷新 → QueryFacade.refresh / refreshAll / refreshByPrefix
           → QueryCacheStore 推进被波及键的世代并清除其条目（含降级副本）
```

## 结果来源（`source` 字段）

| source          | 含义                                             |
|-----------------|--------------------------------------------------|
| `COMPUTED`      | 本次请求触发了底层计算并写入缓存                 |
| `COALESCED`     | 复用了同键正在进行的计算结果（并发合并）         |
| `CACHE`         | 命中未过期缓存                                   |
| `STALE_DEGRADED`| 底层不可用，返回带降级标记的旧值（`degraded=true`）|

## 缓存与降级语义

- **并发合并**：同一时刻对同一键的多个请求只触发一次底层计算，其余请求等待并复用结果；
  计算失败或超时**不写入缓存**，占位立即清除，后续请求重新触发计算。
- **有效期**：条目按 `query-cache.ttl-millis` 过期，过期后不再对外返回；
  `DELETE /api/query/{key}` 显式失效后，旧值连同降级副本一并清除。
- **降级**：仅当底层计算失败/超时且 `query-cache.stale-fallback-enabled=true`
  且缓存中仍有（已过期的）**同世代**旧值时，返回 `STALE_DEGRADED` 结果；
  该结果带 `degraded=true` 标记，与正常结果明确可区分。开关关闭时失败直接抛出。
- **合并等待容量/超时**：同键已有计算在途时，等待复用的请求数受
  `query-cache.coalesce-wait-capacity` 约束（超出立即 `WAIT_OVERLOADED`），
  等待时长受 `query-cache.coalesce-wait-timeout-millis` 约束（超时 `WAIT_TIMEOUT`）。
  等待者自身**从不触发底层计算**，因此不会绕过底层并发上限；
  被拒绝或超时的请求只回滚自己的等待计数，不影响正在进行的那次计算，也不影响其他键。

## 数据刷新驱动失效（后台变更即时生效）

后台独立修改数据后，通过刷新接口通知本地缓存，**即使条目仍在 TTL 内也立即失效**：

- **单键刷新** `POST /api/query/refresh/{key}`：只失效该键。
- **批量刷新** `POST /api/query/refresh`，请求体 `{"keys":["ds-1:s0","ds-1:s1"]}`，
  或按前缀 `{"prefix":"ds-1:"}`：同一批里**没被波及的键继续命中缓存、不被重算**。

每个键维护一个单调递增的**世代（generation）**，规则如下：

1. 刷新只推进**被波及键**的世代并清除其缓存条目（含降级副本）；其他键世代与条目不变。
2. 发起计算时捕获当前世代，计算结束提交时若世代已推进，结果**视为作废**：
   既不写入缓存、也不对外返回，发起方在新世代自动重算（最多 3 次，防止刷新风暴无限重试），
   因此**刷新后的第一次查询必然反映新状态**。
3. 这杜绝了"新结果被旧结果盖回去"：变更前启动、变更后才结束的计算无法污染缓存；
   已在等待的合并请求最终复用的也是重算后的新结果。
4. **与降级的关系**：刷新会清掉旧值副本并推进世代，因此变更后作废的旧值
   **不能再通过降级路径露面**；只有与当前世代一致的旧值才有降级资格。
5. **与 TTL / 显式失效的兼容关系**：
   - TTL 到期：原有行为不变，条目过期后可重算，旧值在被刷新前仍有降级资格；
   - 显式失效（`DELETE`）：只清条目、**不推进世代**，不影响在途计算提交，语义与原来一致；
   - 数据刷新（`POST refresh`）：清条目**且**推进世代，表达"后台数据确实变了"，
     强于显式失效——它会作废所有世代落后的在途结果。

## 失败原因（可区分）

异常 `QueryFailureException.Reason`，HTTP 映射如下：

| reason            | 含义                                   | HTTP |
|-------------------|----------------------------------------|------|
| `OVERLOADED`      | 底层许可排队容量已满，被背压拒绝       | 429  |
| `ACQUIRE_TIMEOUT` | 排队等待底层许可超时                   | 429  |
| `WAIT_OVERLOADED` | 同键等待复用的请求数超过容量，快速拒绝 | 429  |
| `WAIT_TIMEOUT`    | 同键等待复用在途结果超时               | 429  |
| `COMPUTE_TIMEOUT` | 底层计算超时                           | 504  |
| `UPSTREAM_ERROR`  | 底层计算抛错                           | 502  |

`WAIT_*` 与限流层的 `OVERLOADED / ACQUIRE_TIMEOUT` 分开：
前者发生在"同键合并等待"环节（等待者不占底层许可），后者发生在"底层计算并发"环节。
取消或超时的请求都会释放其占用的并发许可（`finally` 中释放，超时任务被中断取消）。

## 配置项（`application.properties`，前缀 `query-cache.`）

| 配置                       | 默认  | 说明                                   |
|----------------------------|-------|----------------------------------------|
| `ttl-millis`               | 1000  | 缓存条目有效期                         |
| `stale-fallback-enabled`   | true  | 底层不可用时是否允许返回降级旧值       |
| `max-concurrency`          | 4     | 底层计算最大并发数（合并/旁路均受限）  |
| `queue-capacity`           | 16    | 排队等待许可的容量，超出即 `OVERLOADED`|
| `acquire-timeout-millis`   | 500   | 排队等待许可的最大时长                 |
| `compute-timeout-millis`   | 2000  | 单次底层计算最大时长                   |
| `coalesce-wait-capacity`   | 32    | 同键等待复用结果的最大请求数（不含发起方），超出即 `WAIT_OVERLOADED` |
| `coalesce-wait-timeout-millis` | 5000 | 同键等待复用在途结果的最大时长，超时即 `WAIT_TIMEOUT` |
| `simulated-delay-millis`   | 100   | 模拟慢依赖的基础延迟                   |

## 接口

- `GET /api/query/{key}` → `{key, value, source, degraded, elapsedMillis}`
- `DELETE /api/query/{key}` → 显式失效该键（不推进世代，在途计算仍可提交）
- `POST /api/query/refresh/{key}` → 单键数据刷新（推进世代，在途旧结果作废）
- `POST /api/query/refresh` → 批量刷新：`{"keys":[...]}` 或 `{"prefix":"..."}`

## 本地验证

```bash
# 运行全部测试（并发合并 / 缓存失效 / 超时降级背压 / 数据刷新 / 在途交错 / 同键等待容量超时）
mvn test

# 启动服务
mvn spring-boot:run

# 观察合并：并发 10 个同键请求只触发一次计算（日志中 invocation# 只出现一次）
seq 1 10 | xargs -P10 -I{} curl -s http://localhost:8080/api/query/burst

# 观察缓存命中与显式失效
curl -s http://localhost:8080/api/query/hello          # COMPUTED
curl -s http://localhost:8080/api/query/hello          # CACHE
curl -s -X DELETE http://localhost:8080/api/query/hello
curl -s http://localhost:8080/api/query/hello          # COMPUTED（新值）

# 观察后台数据刷新即时生效（仍在 TTL 内也必须拿到新值；无关键不受影响）
curl -s http://localhost:8080/api/query/ds-1:s0        # COMPUTED（value-...-1）
curl -s http://localhost:8080/api/query/ds-1:s1        # COMPUTED
curl -s http://localhost:8080/api/query/ds-2:s9        # COMPUTED
curl -s http://localhost:8080/api/query/ds-1:s0        # CACHE
curl -s -X POST http://localhost:8080/api/query/refresh/ds-1:s0
curl -s http://localhost:8080/api/query/ds-1:s0        # COMPUTED（value-...-4，新值）

# 批量刷新（仅这批键重算，ds-2:s9 仍 CACHE）
curl -s -X POST -H 'Content-Type: application/json' \
  -d '{"keys":["ds-1:s0","ds-1:s1"]}' http://localhost:8080/api/query/refresh
curl -s http://localhost:8080/api/query/ds-1:s0        # COMPUTED（新值）
curl -s http://localhost:8080/api/query/ds-2:s9        # CACHE（未被波及）

# 按前缀批量刷新某数据集下的所有分片
curl -s -X POST -H 'Content-Type: application/json' \
  -d '{"prefix":"ds-1:"}' http://localhost:8080/api/query/refresh
```

测试日志会打印每个请求的键、命中/合并结果与耗时（`[test] key=... source=... elapsed=...ms`），
并断言底层计算次数与失败原因。

## 测试覆盖

| 测试类                     | 场景                                                         |
|----------------------------|--------------------------------------------------------------|
| `CoalescingTest`           | 8 线程同键并发 → 底层计算恰好 1 次，1 COMPUTED + 7 COALESCED |
| `CacheInvalidationTest`    | 缓存命中不触发计算；显式失效/TTL 过期后重新计算且不返回旧值  |
| `TimeoutDegradationTest`   | 计算超时降级返回带标记旧值；关闭降级后抛出可区分原因；背压拒绝 |
| `DataRefreshTest`          | 单键/批量/前缀刷新后被波及键拿新值；同批无关键仍命中缓存且不重算 |
| `RefreshRaceTest`          | 刷新与在途计算交错：旧结果不返回也不留存、等待者只拿新值；作废旧值不走降级 |
| `CoalesceCapacityTest`     | 同键等待超容量快速 `WAIT_OVERLOADED`、等待超时 `WAIT_TIMEOUT`；原因可区分且不波及在途计算/其他键 |
