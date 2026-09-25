package com.github.highcumontoa.concurrentquerycachejava.tests;

import com.github.highcumontoa.concurrentquerycachejava.config.QueryCacheProperties;
import com.github.highcumontoa.concurrentquerycachejava.core.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景二：缓存失效 —— 命中、显式失效与 TTL 过期后不得返回过期结果。
 */
class CacheInvalidationTest {

    private static final Logger log = LoggerFactory.getLogger(CacheInvalidationTest.class);

    private QueryCacheProperties props;
    private SlowDependency dependency;
    private QueryFacade facade;

    @BeforeEach
    void setUp() {
        props = new QueryCacheProperties();
        props.setSimulatedDelayMillis(20);
        props.setTtlMillis(5_000);
        QueryCacheStore store = new QueryCacheStore(props);
        dependency = new SlowDependency(props);
        facade = new QueryFacade(props, store, new RequestCoalescer(), new ConcurrencyLimiter(props), dependency);
    }

    @Test
    void explicitInvalidationForcesRecompute() {
        String key = "inv-key";
        QueryResult r1 = facade.query(key);
        log.info("[test] key={} source={} value={} elapsed={}ms", key, r1.source(), r1.value(), r1.elapsedMillis());
        assertEquals(QuerySource.COMPUTED, r1.source());
        assertEquals(1, dependency.invocationCount());

        QueryResult r2 = facade.query(key);
        log.info("[test] key={} source={} value={} elapsed={}ms", key, r2.source(), r2.value(), r2.elapsedMillis());
        assertEquals(QuerySource.CACHE, r2.source(), "未过期应命中缓存");
        assertEquals(r1.value(), r2.value());
        assertEquals(1, dependency.invocationCount(), "命中缓存不得触发计算");

        facade.invalidate(key);
        QueryResult r3 = facade.query(key);
        log.info("[test] key={} source={} value={} elapsed={}ms", key, r3.source(), r3.value(), r3.elapsedMillis());
        assertEquals(QuerySource.COMPUTED, r3.source(), "显式失效后必须重新计算");
        assertNotEquals(r1.value(), r3.value(), "失效后不得返回旧值");
        assertEquals(2, dependency.invocationCount());
    }

    @Test
    void expiredEntryIsNeverServed() throws Exception {
        props.setTtlMillis(150);
        String key = "ttl-key";
        QueryResult r1 = facade.query(key);
        log.info("[test] key={} source={} value={} elapsed={}ms", key, r1.source(), r1.value(), r1.elapsedMillis());
        assertEquals(1, dependency.invocationCount());

        Thread.sleep(250); // 等待过期

        QueryResult r2 = facade.query(key);
        log.info("[test] key={} source={} value={} elapsed={}ms", key, r2.source(), r2.value(), r2.elapsedMillis());
        assertEquals(QuerySource.COMPUTED, r2.source(), "过期后必须重新计算而非返回过期值");
        assertNotEquals(r1.value(), r2.value());
        assertEquals(2, dependency.invocationCount());
    }
}
