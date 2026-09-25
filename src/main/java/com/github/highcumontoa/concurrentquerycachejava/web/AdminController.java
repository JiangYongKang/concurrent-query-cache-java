package com.github.highcumontoa.concurrentquerycachejava.web;

import com.github.highcumontoa.concurrentquerycachejava.backend.SlowBackendService;
import com.github.highcumontoa.concurrentquerycachejava.cache.QueryCache;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 本地验证用管理接口：
 * <ul>
 *   <li>POST /api/admin/invalidate?key=k 显式失效单个键</li>
 *   <li>POST /api/admin/invalidate-all 失效全部</li>
 *   <li>POST /api/admin/backend/failing?value=true|false 切换底层故障</li>
 *   <li>POST /api/admin/backend/delay?millis=n 调整底层延迟</li>
 *   <li>POST /api/admin/backend/bump-version?key=k 模拟底层状态变化</li>
 *   <li>POST /api/admin/reset 重置计数与模拟状态</li>
 *   <li>GET /api/admin/stats 查看调用次数、缓存大小、在途数、剩余计算许可</li>
 * </ul>
 */
@RestController
public class AdminController {

    private final QueryCache<Map<String, Object>> cache;
    private final SlowBackendService backend;

    public AdminController(QueryCache<Map<String, Object>> cache, SlowBackendService backend) {
        this.cache = cache;
        this.backend = backend;
    }

    @PostMapping("/api/admin/invalidate")
    public Map<String, Object> invalidate(@RequestParam("key") String key) {
        cache.invalidate(key);
        return Map.of("action", "invalidate", "key", key);
    }

    @PostMapping("/api/admin/invalidate-all")
    public Map<String, Object> invalidateAll() {
        cache.invalidateAll();
        return Map.of("action", "invalidate-all");
    }

    @PostMapping("/api/admin/backend/failing")
    public Map<String, Object> setFailing(@RequestParam("value") boolean value) {
        backend.setFailing(value);
        return Map.of("failing", backend.isFailing());
    }

    @PostMapping("/api/admin/backend/delay")
    public Map<String, Object> setDelay(@RequestParam("millis") long millis) {
        backend.setDelayMillis(millis);
        return Map.of("delayMillis", backend.getDelayMillis());
    }

    @PostMapping("/api/admin/backend/bump-version")
    public Map<String, Object> bumpVersion(@RequestParam("key") String key) {
        long version = backend.bumpVersion(key);
        // 底层状态变化后，旧缓存对外不得再作为正常结果：立即显式失效
        cache.invalidate(key);
        return Map.of("key", key, "version", version, "invalidate", "done");
    }

    @PostMapping("/api/admin/reset")
    public Map<String, Object> reset() {
        cache.invalidateAll();
        backend.reset();
        return Map.of("reset", true);
    }

    @GetMapping("/api/admin/stats")
    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("backendInvocations", backend.getInvocationCount());
        m.put("backendFailing", backend.isFailing());
        m.put("backendDelayMillis", backend.getDelayMillis());
        m.put("cacheSize", cache.cacheSize());
        m.put("inFlightKeys", cache.inFlightCount());
        m.put("availableComputePermits", cache.availableComputePermits());
        return m;
    }
}
