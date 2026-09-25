# concurrent-query-cache-java

面向高并发读取的本地查询加速层：在 Spring Boot Web 服务内提供
**并发合并 + TTL 缓存 + 并发上限/背压 + 可开关的降级旧值**，
使重复请求、慢依赖与并发压力下的响应时间与状态变化保持可预测。

## 架构

```
请求 → QueryFacade
        ├─ 1. QueryCacheStore.getFresh   命中未过期缓存 → CACHE
        ├─ 2. RequestCoalescer.coalesce  同键并发合并，只放行一个计算
        │     └─ 双重检查缓存（等待期间可能已被同键请求填充）
        ├─ 3. ConcurrencyLimiter.execute 信号量限流 + 排队背压 + 计算超时
        │     └─ SlowDependency.compute  可本地模拟的慢依赖（延迟/故障可注入）
        └─ 4. 失败时：降级开关打开且有旧值 → STALE_DEGRADED，否则抛出可区分原因的异常
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
  且缓存中仍有（已过期的）旧值时，返回 `STALE_DEGRADED` 结果；
  该结果带 `degraded=true` 标记，与正常结果明确可区分。开关关闭时失败直接抛出。

## 失败原因（可区分）

异常 `QueryFailureException.Reason`，HTTP 映射如下：

| reason            | 含义                       | HTTP |
|-------------------|----------------------------|------|
| `OVERLOADED`      | 排队容量已满，被背压拒绝   | 429  |
| `ACQUIRE_TIMEOUT` | 排队等待许可超时           | 429  |
| `COMPUTE_TIMEOUT` | 底层计算超时               | 504  |
| `UPSTREAM_ERROR`  | 底层计算抛错               | 502  |

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
| `simulated-delay-millis`   | 100   | 模拟慢依赖的基础延迟                   |

## 接口

- `GET /api/query/{key}` → `{key, value, source, degraded, elapsedMillis}`
- `DELETE /api/query/{key}` → 显式失效该键

## 本地验证

```bash
# 运行全部测试（并发合并 / 缓存失效 / 超时降级与背压）
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
```

测试日志会打印每个请求的键、命中/合并结果与耗时（`[test] key=... source=... elapsed=...ms`），
并断言底层计算次数与失败原因。

## 测试覆盖

| 测试类                     | 场景                                                         |
|----------------------------|--------------------------------------------------------------|
| `CoalescingTest`           | 8 线程同键并发 → 底层计算恰好 1 次，1 COMPUTED + 7 COALESCED |
| `CacheInvalidationTest`    | 缓存命中不触发计算；显式失效/TTL 过期后重新计算且不返回旧值  |
| `TimeoutDegradationTest`   | 计算超时降级返回带标记旧值；关闭降级后抛出可区分原因；背压拒绝 |
