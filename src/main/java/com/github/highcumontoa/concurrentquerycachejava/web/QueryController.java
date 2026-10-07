package com.github.highcumontoa.concurrentquerycachejava.web;

import com.github.highcumontoa.concurrentquerycachejava.core.QueryFacade;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * 查询与缓存管理接口。
 */
@RestController
@RequestMapping("/api/query")
public class QueryController {

    private final QueryFacade facade;

    public QueryController(QueryFacade facade) {
        this.facade = facade;
    }

    @GetMapping("/{key}")
    public QueryResponse query(@PathVariable String key) {
        return QueryResponse.from(facade.query(key));
    }

    @DeleteMapping("/{key}")
    public Map<String, String> invalidate(@PathVariable String key) {
        facade.invalidate(key);
        return Map.of("invalidated", key);
    }

    /**
     * 模拟后台数据刷新：单键（{key,value}）或批量（{updates:{k:v,...}}）。
     * 刷新立即提升受影响键的数据版本并失效其缓存；未波及的键不受影响。
     */
    @PostMapping("/refresh")
    public Map<String, Object> refresh(@RequestBody RefreshRequest request) {
        if (request.updates() != null && !request.updates().isEmpty()) {
            facade.refreshBatch(request.updates());
            return Map.of("refreshed", request.updates().keySet());
        }
        if (request.key() != null) {
            String value = request.value() != null ? request.value() : "refreshed-" + request.key();
            facade.refresh(request.key(), value);
            return Map.of("refreshed", java.util.List.of(request.key()));
        }
        throw new IllegalArgumentException("either 'updates' (batch) or 'key' (single) is required");
    }
}
