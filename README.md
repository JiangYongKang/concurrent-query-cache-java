# concurrent-query-cache-java

面向高并发读取的本地查询加速层：在 Spring Boot Web 服务内提供
**并发合并 + TTL 缓存 + 并发上限/背压 + 可开关的降级旧值 + 后台刷新驱动失效**，
使重复请求、慢依赖、并发压力与后台数据变更下的响应时间与状态变化保持可预测。

## 架构

```
请求 → QueryFacade
        ├─ 1. QueryCacheStore.getFresh   命中未过期缓存 → CACHE
        ├─ 2. RequestCoalescer.coalesce  同键并发合并，只放行一个计算（等待方有容量/超时上限）
        │     └─ 双重检查缓存（等待期间可能已被同键请求填充）
        ├─ 3. ConcurrencyLimiter.execute 信号量限流 + 排队背压 + 计算超时
        │     └─ SlowDependency.compute  可本地模拟的慢依赖（延迟/故障/后台刷新可注入）
        ├─ 4. putIfVersionMatches        按数据版本 CAS 提交：版本不符的在途旧结果直接丢弃并重算
        └─ 5. 失败时：降级开关打开且有同版本旧值 → STALE_DEGRADED，否则抛出可区分原因的异常

后台 → QueryFacade.refresh / refreshBatch
        ├─ SlowDependency 推送新状态
        └─ QueryCacheStore.refresh  提升受影响键数据版本 + 移除其缓存条目（含降级旧值）
```

## 结果来源（`source` 字段）

| source          | 含义                                             |
|-----------------|--------------------------------------------------|
| `COMPUTED`      | 本次请求触发了底层计算并写入缓存                 |
| `COALESCED`     | 复用了同键正在进行的计算结果（并发合并）         |
| `CACHE`         | 命中未过期缓存                                   |
| `STALE_DEGRADED`| 底层不可用，返回带降级标记的旧值（`degraded=true`）|

## 缓存与降级语义

- **并发合并**：同一时刻对同一键的多个请求只触发一次底层计算，其余请求作为等待方复用结果；
  计算失败或超时**不写入缓存**，占位立即清除，后续请求重新触发计算。
- **有效期**：条目按 `query-cache.ttl-millis` 过期，过期后不再对外返回；
  `DELETE /api/query/{key}` 显式失效后，旧值连同降级副本一并清除。
- **降级**：仅当底层计算失败/超时且 `query-cache.stale-fallback-enabled=true`
  且缓存中仍有（已过期的）**且仍属于当前数据版本**的旧值时，返回 `STALE_DEGRADED` 结果；
  该结果带 `degraded=true` 标记，与正常结果明确可区分。开关关闭时失败直接抛出。
- **后台刷新驱动失效**：见下节。

## 后台数据刷新驱动失效

后台会独立地变更数据，常为同一数据集下的一批相关键（分片）一起刷新。

- **单键刷新** `QueryFacade.refresh(key, newValue)`：向底层推送新状态，并对该键
  提升数据版本 + 移除缓存条目。**即使条目仍在 TTL 有效期内，后续查询也必须拿到新状态**。
- **批量刷新** `QueryFacade.refreshBatch(Map<key,newValue>)`：只作批量内列出的键；
  **同一批变更中没被波及的键继续正常命中缓存，不被重算**。
- **与 TTL / 显式失效的关系**：刷新是独立于 TTL 的失效通道，TTL 未到也立即失效；
  `DELETE` 显式失效同样提升版本并清除旧值；正常缓存命中、显式失效、到期过期行为均保持不变。

### 防止“新结果被旧结果盖回去”（在途计算与刷新交错）

每个键维护单调递增的**数据版本号**；计算启动时记录当前版本，结束时用
`putIfVersionMatches(key, value, 启动版本)` 做 CAS 提交：

- 计算在刷新之前启动、刷新之后才结束 → 提交时版本已推进，**旧结果被丢弃**：
  既不对外返回，也不会成为之后可命中的缓存数据；
- 该次计算的发起方会在合并内部基于新版本**自动重算一次**（上限 3 次），
  因此合并等待方与刷新后的第一个查询拿到的都是新状态；持续刷新导致始终追不上时
  抛出 `REFRESH_CONFLICT`（HTTP 409）；
- 刷新会一并移除旧条目并推进版本，因此**已作废的旧值也不能再通过 `STALE_DEGRADED`
  降级路径露面**；未被刷新的键，其过期旧值仍可正常降级，行为不变。

## 同键合并等待的容量与超时

等待方本身**不占用**底层计算许可（不绕过、也不挤占 `max-concurrency`），并受两个上限保护：

- 等待数超过 `query-cache.coalesce-waiter-capacity`：立即以
  `COALESCER_OVERLOADED` **快速失败**；
- 等待时长超过 `query-cache.coalesce-wait-timeout-millis`：以
  `COALESCE_WAIT_TIMEOUT` 失败。

被拒绝或等超时的等待方干净退出：**不触发底层计算、不影响发起方那一次计算、
不影响其他键、不泄漏任何并发许可**。

## 失败原因（可区分）

异常 `QueryFailureException.Reason`，HTTP 映射如下：

| reason                   | 含义                                   | HTTP |
|--------------------------|----------------------------------------|------|
| `OVERLOADED`             | 底层排队容量已满，被背压拒绝           | 429  |
| `ACQUIRE_TIMEOUT`        | 排队等待底层许可超时                   | 429  |
| `COALESCER_OVERLOADED`   | 同键合并等待方数量超过容量，快速拒绝   | 429  |
| `COMPUTE_TIMEOUT`        | 底层计算超时                           | 504  |
| `COALESCE_WAIT_TIMEOUT`  | 同键等待在途计算结果超时               | 504  |
| `REFRESH_CONFLICT`       | 刷新与在途计算持续交错，结果作废后重算仍非最新 | 409  |
| `UPSTREAM_ERROR`         | 底层计算抛错                           | 502  |

取消或超时的请求都会释放其占用的并发许可（`finally` 中释放，超时任务被中断取消）；
合并等待方不占用底层许可，其超时/拒绝也无需释放许可。

## 配置项（`application.properties`，前缀 `query-cache.`）

| 配置                       | 默认  | 说明                                   |
|----------------------------|-------|----------------------------------------|
| `ttl-millis`               | 1000  | 缓存条目有效期                         |
| `stale-fallback-enabled`   | true  | 底层不可用时是否允许返回降级旧值       |
| `max-concurrency`          | 4     | 底层计算最大并发数（合并/旁路均受限）  |
| `queue-capacity`           | 16    | 排队等待许可的容量，超出即 `OVERLOADED`|
| `acquire-timeout-millis`   | 500   | 排队等待许可的最大时长                 |
| `compute-timeout-millis`   | 2000  | 单次底层计算最大时长                   |
| `simulated-delay-millis`   | 100   | 模拟慢依赖的基础延迟                   |
| `coalesce-waiter-capacity` | 32    | 同键在途计算允许的等待方数量上限（不含发起方），超出即 `COALESCER_OVERLOADED` |
| `coalesce-wait-timeout-millis` | 2500 | 同键等待方等待在途结果的最大时长，超时即 `COALESCE_WAIT_TIMEOUT` |

## 接口

- `GET /api/query/{key}` → `{key, value, source, degraded, elapsedMillis}`
- `DELETE /api/query/{key}` → 显式失效该键
- `POST /api/query/refresh` → 模拟后台数据刷新（立即失效受影响键缓存并提升数据版本）
  - 单键：`{"key":"ds-1-shard-0","value":"v2"}`（省略 `value` 时取默认值 `refreshed-<key>`）
  - 批量：`{"updates":{"ds-1-shard-0":"v2","ds-1-shard-1":"v2"}}`（仅列出的键失效，其余命中）

## 本地验证

```bash
# 运行全部测试（并发合并 / 缓存失效 / 超时降级与背压 / 刷新驱动失效 / 等待容量超时）
mvn test

# 启动服务
mvn spring-boot:run

# 观察合并：并发 10 个同键请求只触发一次计算（日志中 invocation# 只出现一次）
seq 1 10 | xargs -P10 -I{} curl -s http://localhost:8080/api/query/burst

# 观察缓存命中与失效
curl -s http://localhost:8080/api/query/hello          # COMPUTED
curl -s http://localhost:8080/api/query/hello          # CACHE
curl -s -X DELETE http://localhost:8080/api/query/hello
curl -s http://localhost:8080/api/query/hello          # COMPUTED（新值）

# 观察批量刷新：TTL 内立即取新值；未波及的 ds-shard-2 仍 CACHE
curl -s http://localhost:8080/api/query/ds-shard-0
curl -s http://localhost:8080/api/query/ds-shard-1
curl -s http://localhost:8080/api/query/ds-shard-2
curl -s -X POST http://localhost:8080/api/query/refresh \
  -H 'Content-Type: application/json' \
  -d '{"updates":{"ds-shard-0":"v2","ds-shard-1":"v2"}}'
curl -s http://localhost:8080/api/query/ds-shard-0     # COMPUTED，value=v2
curl -s http://localhost:8080/api/query/ds-shard-1     # COMPUTED，value=v2
curl -s http://localhost:8080/api/query/ds-shard-2     # CACHE（未受影响）
```

测试日志会打印每个请求的键、结果来源与耗时（`[test] key=... source=... value=... elapsed=...ms`），
并断言底层实际计算次数（`invocations=...`）与可区分的失败原因。

## 测试覆盖

| 测试类                     | 场景                                                         |
|----------------------------|--------------------------------------------------------------|
| `CoalescingTest`           | 8 线程同键并发 → 底层计算恰好 1 次，1 COMPUTED + 7 COALESCED |
| `CacheInvalidationTest`    | 缓存命中不触发计算；显式失效/TTL 过期后重新计算且不返回旧值  |
| `TimeoutDegradationTest`   | 计算超时降级返回带标记旧值；关闭降级后抛出可区分原因；背压拒绝 |
| `DataRefreshTest`          | 批量/单键刷新后 TTL 内即取新值且无关键仍命中（断言计算次数）；在途计算遇刷新旧结果作废并重算；合并等待方同样拿新值；作废旧值不再降级露面而未刷新键仍可降级 |
| `CoalescerCapacityTest`    | 等待超容量快速 `COALESCER_OVERLOADED`（同键仅 1 次计算）；等待超时 `COALESCE_WAIT_TIMEOUT` 且不影响发起方/其他键；拒绝与超时等待方不泄漏底层许可 |
