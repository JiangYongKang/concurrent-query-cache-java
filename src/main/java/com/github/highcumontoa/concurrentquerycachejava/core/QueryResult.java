package com.github.highcumontoa.concurrentquerycachejava.core;

/**
 * 查询结果：值 + 来源标记 + 耗时。
 */
public record QueryResult(String key, String value, QuerySource source, long elapsedMillis) {

    public boolean degraded() {
        return source == QuerySource.STALE_DEGRADED;
    }
}
