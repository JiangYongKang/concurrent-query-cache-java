package com.github.highcumontoa.concurrentquerycachejava.core;

import com.github.highcumontoa.concurrentquerycachejava.config.QueryCacheProperties;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 带 TTL 与数据世代（generation）的本地缓存：
 * 过期、显式失效或后台数据刷新后不再对外返回；
 * 过期条目仍保留在内部，供降级开关打开时返回带标记的旧值，
 * 但一旦该键发生数据刷新，旧条目连同其降级资格一并清除。
 *
 * <p>世代用于防止"旧结果盖回新结果"：计算开始时捕获世代，
 * 提交时若世代已被刷新推进，则结果作废、不得写入也不得对外返回。
 * 同一时刻同一键只有一个计算（由 {@link RequestCoalescer} 保证），
 * 因此单键内部用槽位上的串行锁即可保证 检查-提交 原子。
 */
@Component
public class QueryCacheStore {

    private record Entry(String value, long expiresAtMillis, long generation) {
    }

    /** 单键槽位：串行化该键的读/写/失效/刷新。 */
    private static final class Slot {
        private final AtomicLong generation = new AtomicLong(0);
        private Entry entry;
    }

    private final QueryCacheProperties properties;
    private final Map<String, Slot> slots = new ConcurrentHashMap<>();

    public QueryCacheStore(QueryCacheProperties properties) {
        this.properties = properties;
    }

    private Slot slot(String key) {
        return slots.computeIfAbsent(key, k -> new Slot());
    }

    /** 读取未过期条目。 */
    public Optional<String> getFresh(String key) {
        synchronized (slot(key)) {
            Entry entry = slot(key).entry;
            if (entry == null || System.currentTimeMillis() >= entry.expiresAtMillis()) {
                return Optional.empty();
            }
            return Optional.of(entry.value());
        }
    }

    /**
     * 读取条目（含已过期），用于降级返回旧值。
     * 仅当条目世代等于当前世代（即该键未被刷新过）时才可用。
     */
    public Optional<String> getStale(String key) {
        Slot slot = slot(key);
        synchronized (slot) {
            Entry entry = slot.entry;
            if (entry == null || entry.generation() != slot.generation.get()) {
                return Optional.empty();
            }
            return Optional.of(entry.value());
        }
    }

    /** 当前世代；计算启动时捕获，提交时比对。 */
    public long currentGeneration(String key) {
        return slot(key).generation.get();
    }

    /**
     * 仅当 {@code generation} 仍是该键当前世代时提交结果。
     *
     * @return true 表示已写入；false 表示期间发生过数据刷新，结果已作废
     */
    public boolean commitIfCurrent(String key, long generation, String value) {
        Slot slot = slot(key);
        synchronized (slot) {
            if (generation != slot.generation.get()) {
                return false;
            }
            slot.entry = new Entry(value, System.currentTimeMillis() + properties.getTtlMillis(), generation);
            return true;
        }
    }

    /** 写入条目（按当前世代提交，主要用于无世代感知的兼容路径）。 */
    public void put(String key, String value) {
        Slot slot = slot(key);
        synchronized (slot) {
            slot.entry = new Entry(value, System.currentTimeMillis() + properties.getTtlMillis(),
                    slot.generation.get());
        }
    }

    /**
     * 显式失效某个键（含旧值，失效后不再可降级返回）。
     * 不推进世代：不影响正在进行的计算结果提交，保持原有语义。
     */
    public void invalidate(String key) {
        Slot slot = slot(key);
        synchronized (slot) {
            slot.entry = null;
        }
    }

    /**
     * 数据刷新驱动失效：推进该键世代并清除现有条目（含降级副本）。
     * 世代推进前启动的在途计算，其结果提交时会被判为作废。
     */
    public void refresh(String key) {
        Slot slot = slot(key);
        synchronized (slot) {
            slot.entry = null;
            slot.generation.incrementAndGet();
        }
    }

    /** 批量刷新同一批相关键；未被波及的键不受影响。 */
    public void refreshAll(Collection<String> keys) {
        for (String key : keys) {
            refresh(key);
        }
    }

    /** 按前缀刷新一组相关键（如 dataset-1 下的所有分片 dataset-1:shard-*）。 */
    public int refreshByPrefix(String prefix) {
        int affected = 0;
        for (String key : slots.keySet()) {
            if (key.startsWith(prefix)) {
                refresh(key);
                affected++;
            }
        }
        return affected;
    }

    /** 清空全部条目。 */
    public void clear() {
        slots.clear();
    }
}
