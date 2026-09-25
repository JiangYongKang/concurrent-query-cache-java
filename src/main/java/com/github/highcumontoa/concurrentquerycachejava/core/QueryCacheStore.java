package com.github.highcumontoa.concurrentquerycachejava.core;

import com.github.highcumontoa.concurrentquerycachejava.config.QueryCacheProperties;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 带 TTL 的本地缓存：过期或显式失效后不再对外返回；
 * 过期条目仍保留在内部，供降级开关打开时返回带标记的旧值。
 */
@Component
public class QueryCacheStore {

    private record Entry(String value, long expiresAtMillis) {
    }

    private final QueryCacheProperties properties;
    private final Map<String, Entry> entries = new ConcurrentHashMap<>();

    public QueryCacheStore(QueryCacheProperties properties) {
        this.properties = properties;
    }

    /** 读取未过期条目。 */
    public Optional<String> getFresh(String key) {
        Entry entry = entries.get(key);
        if (entry == null || System.currentTimeMillis() >= entry.expiresAtMillis()) {
            return Optional.empty();
        }
        return Optional.of(entry.value());
    }

    /** 读取条目（含已过期），用于降级返回旧值。 */
    public Optional<String> getStale(String key) {
        return Optional.ofNullable(entries.get(key)).map(Entry::value);
    }

    /** 写入条目并刷新过期时间。 */
    public void put(String key, String value) {
        entries.put(key, new Entry(value, System.currentTimeMillis() + properties.getTtlMillis()));
    }

    /** 显式失效某个键（含旧值，失效后不再可降级返回）。 */
    public void invalidate(String key) {
        entries.remove(key);
    }

    /** 清空全部条目。 */
    public void clear() {
        entries.clear();
    }
}
