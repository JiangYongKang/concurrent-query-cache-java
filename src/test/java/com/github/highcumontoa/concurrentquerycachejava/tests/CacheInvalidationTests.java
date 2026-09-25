package com.github.highcumontoa.concurrentquerycachejava.tests;

import com.github.highcumontoa.concurrentquerycachejava.cache.CacheStatus;
import com.github.highcumontoa.concurrentquerycachejava.cache.QueryCache;
import com.github.highcumontoa.concurrentquerycachejava.cache.QueryResult;
import com.github.highcumontoa.concurrentquerycachejava.config.QueryCacheProperties;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * 场景二：缓存有效期与失效语义。
 * 断言：TTL 内命中不触发计算；过期后必须重算且不返回旧值；
 * 显式失效后必须重算；底层状态变化（版本变化）后对外不得返回旧结果。
 */
class CacheInvalidationTests {

    private static final Logger log = LoggerFactory.getLogger(CacheInvalidationTests.class);

    private QueryCacheProperties props(long ttl) {
        QueryCacheProperties p = new QueryCacheProperties();
        p.setTtlMillis(ttl);
        p.setStaleAcceptedMillis(60_000);
        p.setStaleOnFailureAllowed(false);
        p.setComputeConcurrency(4);
        p.setMaxWaitersPerKey(50);
        p.setWaitTimeoutMillis(3_000);
        p.setComputeTimeoutMillis(3_000);
        return p;
    }

    private QueryResult<String> fetch(QueryCache<String> cache, String key,
                                      AtomicInteger invocations, String value) {
        QueryResult<String> r = cache.get(key, () -> {
            invocations.incrementAndGet();
            return value;
        });
        log.info("[TEST] key={} status={} coalesced={} value={} elapsedMs={}",
                key, r.getStatus(), r.isCoalesced(), r.getValue(), r.getElapsedMillis());
        return r;
    }

    @Test
    void entryExpiresAfterTtlAndTriggersRecompute() throws InterruptedException {
        QueryCache<String> cache = new QueryCache<>(props(300));
        AtomicInteger invocations = new AtomicInteger();
        String key = "k-ttl";

        QueryResult<String> first = fetch(cache, key, invocations, "v1");
        assertEquals(CacheStatus.COMPUTED, first.getStatus());
        assertEquals("v1", first.getValue());

        QueryResult<String> hit = fetch(cache, key, invocations, "v1");
        assertEquals(CacheStatus.HIT, hit.getStatus(), "TTL 内必须直接命中");
        assertEquals(1, invocations.get(), "命中不得触发底层计算");

        Thread.sleep(500);

        QueryResult<String> after = fetch(cache, key, invocations, "v2");
        assertEquals(CacheStatus.COMPUTED, after.getStatus(), "过期后不得作为命中返回");
        assertFalse(after.isDegraded(), "开关关闭时过期旧值不得降级");
        assertEquals("v2", after.getValue(), "必须拿到重算后的新值，不得返回过期旧值");
        assertEquals(2, invocations.get(), "过期后必须重新触发一次底层计算");
        cache.shutdown();
    }

    @Test
    void explicitInvalidationForcesRecompute() {
        QueryCache<String> cache = new QueryCache<>(props(60_000));
        AtomicInteger invocations = new AtomicInteger();
        String key = "k-invalidate";

        fetch(cache, key, invocations, "v1");
        assertEquals(1, invocations.get());
        fetch(cache, key, invocations, "v1");
        assertEquals(1, invocations.get(), "失效前应命中缓存");

        cache.invalidate(key);
        log.info("[TEST] key={} explicitly invalidated", key);

        QueryResult<String> after = fetch(cache, key, invocations, "v1-next");
        assertEquals(CacheStatus.COMPUTED, after.getStatus(), "显式失效后必须重算");
        assertEquals(2, invocations.get(), "显式失效后必须重新触发底层计算");
        cache.shutdown();
    }

    @Test
    void invalidateAllClearsEveryKey() {
        QueryCache<String> cache = new QueryCache<>(props(60_000));
        AtomicInteger invocations = new AtomicInteger();
        fetch(cache, "a", invocations, "1");
        fetch(cache, "b", invocations, "2");
        assertEquals(2, cache.cacheSize());
        cache.invalidateAll();
        log.info("[TEST] invalidate-all issued, cacheSize={}", cache.cacheSize());
        assertEquals(0, cache.cacheSize());
        fetch(cache, "a", invocations, "1");
        fetch(cache, "b", invocations, "2");
        assertEquals(4, invocations.get(), "全部失效后每个键都必须重新计算");
        cache.shutdown();
    }

    @Test
    void backendStateChangeIsNotServedAsStaleNormalResult() throws InterruptedException {
        // TTL 极短：模拟“缓存条目已过期且底层版本已变化”，对外必须拿到新版本
        QueryCache<String> cache = new QueryCache<>(props(150));
        AtomicInteger invocations = new AtomicInteger();
        String key = "k-versioned";

        QueryResult<String> v1 = fetch(cache, key, invocations, "v1");
        assertEquals("v1", v1.getValue());

        Thread.sleep(250);
        // 底层状态变化：计算结果变为 v2
        QueryResult<String> v2 = fetch(cache, key, invocations, "v2");
        assertEquals(CacheStatus.COMPUTED, v2.getStatus());
        assertEquals("v2", v2.getValue(), "底层状态变化后不得继续返回旧值");
        assertEquals(2, invocations.get());

        QueryResult<String> hitV2 = fetch(cache, key, invocations, "v2");
        assertEquals(CacheStatus.HIT, hitV2.getStatus());
        assertEquals("v2", hitV2.getValue());
        assertEquals(2, invocations.get(), "新版本进入缓存后应命中，不再触发计算");
        cache.shutdown();
    }
}
