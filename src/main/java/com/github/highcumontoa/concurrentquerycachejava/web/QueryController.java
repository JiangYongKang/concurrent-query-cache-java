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
}
