package com.github.highcumontoa.concurrentquerycachejava.core;

import com.github.highcumontoa.concurrentquerycachejava.config.QueryCacheProperties;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 带 TTL 与数据版本号的本地缓存：
 * 过期、显式失效或后台刷新后不再对外返回；
 * 过期条目仍保留在内部（版本仍为最新时）供降级返回旧值；
 * 数据版本号用于防止“变更前启动、变更后结束”的在途计算把旧结果盖回新状态。
 */
@Component
public class QueryCacheStore {

    /** 缓存条目：值 + 计算启动时看到的数据版本 + 过期时间。 */
    private record Entry(String value, long version, long expiresAtMillis) {
    }

    /** 对外暴露的缓存视图（含版本，供计算提交时做 CAS 校验）。 */
    public record Snapshot(String value, long version) {
    }

    private final QueryCacheProperties properties;
    private final Map<String, Entry> entries = new ConcurrentHashMap<>();
    private final Map<String, Long> versions = new ConcurrentHashMap<>();

    public QueryCacheStore(QueryCacheProperties properties) {
        this.properties = properties;
    }

    /** 读取未过期条目。 */
    public Optional<Snapshot> getFresh(String key) {
        Entry entry = entries.get(key);
        if (entry == null || System.currentTimeMillis() >= entry.expiresAtMillis()) {
            return Optional.empty();
        }
        return Optional.of(new Snapshot(entry.value(), entry.version()));
    }

    /** 读取条目（含已过期），仅当条目仍属于当前数据版本时才用于降级返回旧值。 */
    public Optional<String> getStale(String key) {
        Entry entry = entries.get(key);
        if (entry == null || entry.version() != currentVersion(key)) {
            return Optional.empty();
        }
        return Optional.of(entry.value());
    }

    /** 查询某键当前的数据版本（首次出现为 0）。 */
    public long currentVersion(String key) {
        return versions.getOrDefault(key, 0L);
    }

    /**
     * 仅当数据版本仍等于 expectedVersion 时才写入条目（CAS 提交）。
     * 计算在 expectedVersion 下启动；期间若该键被刷新/失效，版本推进，提交被拒绝，旧结果被丢弃。
     *
     * @return true 表示提交成功；false 表示期间发生变更，结果已作废
     */
    public boolean putIfVersionMatches(String key, String value, long expectedVersion) {
        boolean[] accepted = {false};
        entries.compute(key, (k, existing) -> {
            long current = currentVersion(k);
            if (current != expectedVersion) {
                // 期间发生过刷新/失效：不写入，作废结果；若已有更新条目则原样保留
                accepted[0] = false;
                return existing;
            }
            accepted[0] = true;
            return new Entry(value, expectedVersion, System.currentTimeMillis() + properties.getTtlMillis());
        });
        return accepted[0];
    }

    /** 显式失效某个键（含旧值，失效后不再可降级返回）。 */
    public void invalidate(String key) {
        bumpVersion(key);
        entries.remove(key);
    }

    /**
     * 后台数据刷新驱动的失效：移除这批键的缓存条目（含旧值，不可再降级），
     * 并推进其数据版本，使变更前启动的在途计算提交失败。不在批次内的键不受影响。
     */
    public void refresh(Collection<String> keys) {
        keys.forEach(key -> {
            bumpVersion(key);
            entries.remove(key);
        });
    }

    private void bumpVersion(String key) {
        versions.merge(key, 1L, Long::sum);
    }

    /** 清空全部条目。 */
    public void clear() {
        entries.clear();
    }
}
