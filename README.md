# concurrent-query-cache-java

面向高并发读取的**本地查询加速层**（single-flight 合并 + TTL 缓存 + 可开关降级 + 并发上限/背压），
构建在 Spring Boot 3 / Java 21 Web 服务之上，底层依赖为可本地模拟的慢计算 `SlowBackendService`。

## 1. 核心语义

### 1.1 相同查询的并发合并（single-flight）
- 同一时刻对**同一个键**的多个请求只会触发**一次**底层计算：
  第一个请求（leader）执行计算，其余请求（waiter）登记到该键的在途表并等待同一个结果。
- 合并返回的结果带 `coalesced=true` 标记，leader 为 `false`。
- 不同键互不影响。

### 1.2 计算失败 / 超时不写缓存
- 只有底层计算**成功**时才写入缓存。
- 计算异常（`COMPUTE_ERROR`）或计算超时（`COMPUTE_TIMEOUT`）一律不写缓存，
  因此下一个请求会**重新触发**一次底层计算。
- 计算超时会向底层慢任务发出中断（`Future.cancel(true)`），被取消/超时的任务
  不继续占用底层资源；计算许可在 `finally` 中归还。

### 1.3 缓存有效期与失效
条目按时间分为三段：

| 时间段 | 对外行为 |
|---|---|
| `createdAt ~ createdAt+ttl` | **新鲜**：直接返回 `HIT`，不触发计算 |
| `expireAt ~ expireAt+staleAccepted` | **过期旧值**：正常请求会重算；仅在“允许降级”且重算失败时可作为 `STALE_FALLBACK` 返回 |
| 超过 `staleUntil` | 旧值彻底不可用，重算失败即失败 |

- 过期条目**绝不**作为正常结果返回。
- 显式失效：`POST /api/admin/invalidate?key=...`（单键）、`POST /api/admin/invalidate-all`。
- 底层状态变化：`POST /api/admin/backend/bump-version?key=...` 会使版本 +1 **并立即失效**该键，
  之后的请求拿到新版本值，不会继续对外返回旧状态。

### 1.4 降级（返回旧值）——可开关且可区分
- 总开关 `query-cache.stale-on-failure-allowed`（默认 **false**）。
- 关闭时：底层失败/超时直接返回失败，不返回旧值。
- 开启时：重算失败/超时，且该键存在落在降级宽限窗口（`stale-accepted-millis`）内的旧值时，
  返回旧值，但：
  - 响应 `status = STALE_FALLBACK`；
  - 响应 `degraded = true`，与 `HIT` / `COMPUTED` 正常结果**明确可区分**；
  - 日志级别 WARN，含 `STALE_FALLBACK` 与原始失败原因。
- 底层恢复后的下一次请求正常重算并回到 `COMPUTED`。

### 1.5 并发上限与背压
- **计算并发上限** `query-cache.compute-concurrency`：全局公平信号量，
  底层计算的实际并发绝不超过该值（缓存命中与排队等待不占用许可）。
- **单键等待队列容量** `query-cache.max-waiters-per-key`：超过容量的请求
  **立即**以 `QUEUE_FULL` 快速失败，不排队阻塞。
- **等待超时** `query-cache.wait-timeout-millis`：等待者（及 leader 获取许可）
  超过该时间以 `WAIT_TIMEOUT` 失败；失败后其排队槽位被释放，后续请求可再次入队。
- 所有失败原因可区分：`QUEUE_FULL` / `WAIT_TIMEOUT` / `COMPUTE_TIMEOUT` / `COMPUTE_ERROR` / `CANCELLED`。

HTTP 映射（见 `QueryController`）：

| 失败原因 | HTTP 状态码 |
|---|---|
| `QUEUE_FULL` | 429 Too Many Requests |
| `WAIT_TIMEOUT` / `COMPUTE_TIMEOUT` | 504 Gateway Timeout |
| `COMPUTE_ERROR` | 502 Bad Gateway |
| `CANCELLED` | 503 Service Unavailable |

## 2. 配置项（`src/main/resources/application.properties`，前缀 `query-cache`）

| 配置项 | 默认值 | 含义 |
|---|---|---|
| `ttl-millis` | 5000 | 缓存有效期，期内命中 `HIT` |
| `stale-accepted-millis` | 15000 | 过期旧值可用于降级的宽限窗口 |
| `stale-on-failure-allowed` | false | 底层不可用时是否允许降级返回旧值（总开关） |
| `compute-concurrency` | 2 | 底层计算全局并发上限 |
| `max-waiters-per-key` | 10 | 单键合并等待队列容量（不含正在计算的 leader） |
| `wait-timeout-millis` | 1000 | 合并等待 / 获取计算许可的超时 |
| `compute-timeout-millis` | 2000 | 单次底层计算超时；超时不写缓存并中断任务 |

## 3. HTTP 接口

- `GET /api/query?key=k`：查询。响应字段：
  `key, status(HIT|COMPUTED|STALE_FALLBACK|FAILED), degraded, coalesced, elapsedMillis, data[, reason]`。
- 管理/模拟：
  - `POST /api/admin/invalidate?key=k`
  - `POST /api/admin/invalidate-all`
  - `POST /api/admin/backend/failing?value=true|false`（切换底层故障）
  - `POST /api/admin/backend/delay?millis=n`（调整底层延迟）
  - `POST /api/admin/backend/bump-version?key=k`（模拟底层状态变化并失效缓存）
  - `POST /api/admin/reset`（重置计数与模拟状态）
  - `GET  /api/admin/stats`（底层调用次数、缓存大小、在途键数、剩余计算许可）

## 4. 本地验证

### 4.1 运行单元测试（推荐）

```bash
mvn test
```

覆盖三类必测场景（共 16 个测试），**断言底层计算次数与预期一致、失败原因可区分**：

- `ConcurrentCoalescingTests`：20 并发同键 → 底层仅调用 1 次，19 个 `coalesced=true`；
  不同键不合并；计算失败不缓存、下一次重试；新鲜命中不再调用底层。
- `CacheInvalidationTests`：TTL 过期后重算且不返回旧值；显式失效/全量失效；
  底层版本变化后对外只返回新值。
- `TimeoutAndFallbackTests`：计算超时不写缓存且中断任务、释放许可；降级开关关闭/开启两态
  与 `STALE_FALLBACK/degraded=true` 标记；6 键下底层实际并发恒等于上限；
  队列超容量立即 `QUEUE_FULL`；等待超时 `WAIT_TIMEOUT` 后槽位释放可再入队。
- `WebApiIntegrationTests`：端到端校验状态字段与 HTTP 状态码映射。

测试与缓存层均通过 SLF4J 打印**请求键、命中/合并/降级结果、失败原因与耗时(ms)**，
例如：

```
key=k-coalesce result=COMPUTED coalesced=true elapsedMs=403
key=k-timeout result=FAILED reason=COMPUTE_TIMEOUT elapsedMs=301
key=k-stale result=STALE_FALLBACK reason=COMPUTE_ERROR elapsedMs=1
```

### 4.2 手工 curl 验证

```bash
mvn spring-boot:run            # server.port=0 时查看启动日志中的随机端口；
                               # 也可在 application.properties 固定 server.port=8080

# 首次计算 / 第二次命中
curl -s 'http://localhost:8080/api/query?key=foo'
curl -s 'http://localhost:8080/api/query?key=foo'

# 并发合并：同一键 20 并发，stats 中 backendInvocations 只增加 1
for i in $(seq 1 20); do curl -s 'http://localhost:8080/api/query?key=bar' & done; wait
curl -s http://localhost:8080/api/admin/stats

# 显式失效后重算
curl -s -X POST 'http://localhost:8080/api/admin/invalidate?key=foo'
curl -s 'http://localhost:8080/api/query?key=foo'

# 底层失败：默认 502 + reason=COMPUTE_ERROR（降级关闭）
curl -s -X POST 'http://localhost:8080/api/admin/backend/failing?value=true'
curl -s -i 'http://localhost:8080/api/query?key=foo'
curl -s -X POST 'http://localhost:8080/api/admin/backend/failing?value=false'

# 慢依赖 + 超时：把延迟调到大于 compute-timeout，得到 504 + reason=COMPUTE_TIMEOUT
curl -s -X POST 'http://localhost:8080/api/admin/backend/delay?millis=5000'
curl -s -i 'http://localhost:8080/api/query?key=slow'
curl -s -X POST 'http://localhost:8080/api/admin/reset'

# 底层状态变化：版本 +1，缓存随之失效
curl -s -X POST 'http://localhost:8080/api/admin/backend/bump-version?key=foo'
curl -s 'http://localhost:8080/api/query?key=foo'
```

> 手工验证降级：将 `query-cache.stale-on-failure-allowed=true`，先查询得到新鲜值，
> 等待其超过 TTL（但不超过 `ttl + stale-accepted`），再打开底层故障后查询，
> 应得到 200 且 `status=STALE_FALLBACK, degraded=true`。

## 5. 代码结构

```
cache/
  QueryCache.java          加速层核心：合并、TTL、降级、信号量并发上限、队列背压、超时/取消资源释放
  CacheEntry.java          新鲜/过期/降级宽限时间窗
  QueryResult.java         status + degraded + coalesced + reason + elapsedMillis
  InFlight.java            同键在途计算与等待者槽位 CAS
  CacheStatus / FailureReason / CacheQueryException
backend/SlowBackendService.java  可本地模拟的慢依赖（延迟、故障、版本、调用计数）
config/QueryCacheProperties.java / QueryCacheConfig.java
web/QueryController.java   /api/query，失败原因 -> HTTP 状态码
web/AdminController.java   失效、故障/延迟/版本模拟、统计
```
