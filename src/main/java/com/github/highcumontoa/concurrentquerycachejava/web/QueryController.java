package com.github.highcumontoa.concurrentquerycachejava.web;

import com.github.highcumontoa.concurrentquerycachejava.backend.SlowBackendService;
import com.github.highcumontoa.concurrentquerycachejava.cache.CacheQueryException;
import com.github.highcumontoa.concurrentquerycachejava.cache.QueryCache;
import com.github.highcumontoa.concurrentquerycachejava.cache.QueryResult;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 查询接口：GET /api/query?key=k
 *
 * <p>响应体包含 status（HIT/COMPUTED/STALE_FALLBACK/FAILED）、degraded 降级标记、
 * coalesced 是否合并、elapsedMillis 耗时与数据；失败时还带 reason 与 429/504 状态码，
 * 使排队背压与超时在 HTTP 层面也可区分。
 */
@RestController
public class QueryController {

    private final QueryCache<Map<String, Object>> cache;
    private final SlowBackendService backend;

    public QueryController(QueryCache<Map<String, Object>> cache, SlowBackendService backend) {
        this.cache = cache;
        this.backend = backend;
    }

    @GetMapping("/api/query")
    public ResponseEntity<Map<String, Object>> query(@RequestParam("key") String key) {
        QueryResult<Map<String, Object>> result = cache.get(key, () -> {
            try {
                return backend.query(key);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CacheQueryException(
                        com.github.highcumontoa.concurrentquerycachejava.cache.FailureReason.CANCELLED,
                        "backend interrupted: " + key, e);
            }
        });

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("key", key);
        body.put("status", result.getStatus().name());
        body.put("degraded", result.isDegraded());
        body.put("coalesced", result.isCoalesced());
        body.put("elapsedMillis", result.getElapsedMillis());
        body.put("data", result.getValue());

        if (!result.isSuccess()) {
            body.put("reason", result.getReason().name());
            HttpStatus http = switch (result.getReason()) {
                case QUEUE_FULL -> HttpStatus.TOO_MANY_REQUESTS;       // 429 背压快速失败
                case WAIT_TIMEOUT, COMPUTE_TIMEOUT -> HttpStatus.GATEWAY_TIMEOUT; // 504 超时
                case CANCELLED -> HttpStatus.SERVICE_UNAVAILABLE;      // 503 取消
                case COMPUTE_ERROR -> HttpStatus.BAD_GATEWAY;          // 502 底层失败
            };
            return ResponseEntity.status(http).body(body);
        }
        return ResponseEntity.ok(body);
    }
}
