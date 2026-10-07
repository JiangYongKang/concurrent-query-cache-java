package com.github.highcumontoa.concurrentquerycachejava.web;

import com.github.highcumontoa.concurrentquerycachejava.core.QueryFacade;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
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

    /** 后台数据刷新：单键立即失效，在途旧结果作废。 */
    @PostMapping("/refresh/{key}")
    public Map<String, String> refresh(@PathVariable String key) {
        facade.refresh(key);
        return Map.of("refreshed", key);
    }

    /**
     * 后台批量刷新同一批相关键：
     * POST /api/query/refresh  body: {"keys":["dataset-1:shard-0", ...]}
     * 或按前缀：               body: {"prefix":"dataset-1:"}
     */
    @PostMapping("/refresh")
    public ResponseEntity<Map<String, Object>> refreshBatch(@RequestBody RefreshRequest request) {
        if (request.keys() != null && !request.keys().isEmpty()) {
            facade.refreshAll(request.keys());
            return ResponseEntity.ok(Map.of("refreshedKeys", request.keys(), "affected", request.keys().size()));
        }
        if (request.prefix() != null && !request.prefix().isEmpty()) {
            int affected = facade.refreshByPrefix(request.prefix());
            return ResponseEntity.ok(Map.of("refreshedPrefix", request.prefix(), "affected", affected));
        }
        return ResponseEntity.badRequest().body(Map.of("error", "keys or prefix is required"));
    }

    /** 批量刷新请求体。 */
    public record RefreshRequest(List<String> keys, String prefix) {
    }
}
