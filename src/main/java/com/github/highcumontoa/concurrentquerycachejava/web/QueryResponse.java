package com.github.highcumontoa.concurrentquerycachejava.web;

import com.github.highcumontoa.concurrentquerycachejava.core.QueryResult;

/**
 * 查询接口响应体。
 */
public record QueryResponse(String key, String value, String source, boolean degraded, long elapsedMillis) {

    public static QueryResponse from(QueryResult result) {
        return new QueryResponse(result.key(), result.value(),
                result.source().name(), result.degraded(), result.elapsedMillis());
    }
}
