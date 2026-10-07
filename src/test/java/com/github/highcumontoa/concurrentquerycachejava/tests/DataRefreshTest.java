package com.github.highcumontoa.concurrentquerycachejava.tests;

import com.github.highcumontoa.concurrentquerycachejava.config.QueryCacheProperties;
import com.github.highcumontoa.concurrentquerycachejava.core.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景四：后台数据刷新驱动失效 ——
 * 单键/批量刷新后，即使条目仍在 TTL 内也必须拿到新值；
 * 同一批未被波及的键继续命中缓存，不被重算。
 */
class DataRefreshTest {

    private static final Logger log = LoggerFactory.getLogger(DataRefreshTest.class);

    private QueryCacheProperties props;
    private SlowDependency dependency;
    private QueryFacade facade;

    @BeforeEach
    void setUp() {
        props = new QueryCacheProperties();
        props.setSimulatedDelayMillis(20);
        props.setTtlMillis(60_000); // 远大于测试时长，确保不是 TTL 导致的重算
        QueryCacheStore store = new QueryCacheStore(props);
        dependency = new SlowDependency(props);
        facade = new QueryFacade(props, store, new RequestCoalescer(), new ConcurrencyLimiter(props), dependency);
    }

    private QueryResult q(String key) {
        QueryResult r = facade.query(key);
        log.info("[test] key={} source={} value={} elapsed={}ms", r.key(), r.source(), r.value(), r.elapsedMillis());
        return r;
    }

    @Test
    void batchRefreshUpdatesAffectedKeysAndKeepsUnrelatedCached() {
        String s0 = "dataset-1:shard-0";
        String s1 = "dataset-1:shard-1";
        String s2 = "dataset-1:shard-2";
        String other = "dataset-2:shard-9";

        Map<String, QueryResult> first = Map.of(
                s0, q(s0), s1, q(s1), s2, q(s2), other, q(other));
        assertEquals(4, dependency.invocationCount(), "四个键首次查询各计算一次");

        // 再次查询全部命中缓存
        for (String key : first.keySet()) {
            assertEquals(QuerySource.CACHE, q(key).source(), "TTL 内应命中缓存: " + key);
        }
        assertEquals(4, dependency.invocationCount(), "命中缓存不得触发计算");

        // 后台只刷新同一数据集中的前两个分片
        facade.refreshAll(List.of(s0, s1));
        log.info("[test] batch refreshed keys=[{}, {}]", s0, s1);

        QueryResult n0 = q(s0);
        QueryResult n1 = q(s1);
        QueryResult n2 = q(s2);
        QueryResult nOther = q(other);

        assertEquals(QuerySource.COMPUTED, n0.source(), "被刷新的键必须重算");
        assertEquals(QuerySource.COMPUTED, n1.source(), "被刷新的键必须重算");
        assertNotEquals(first.get(s0).value(), n0.value(), "刷新后不得返回旧值");
        assertNotEquals(first.get(s1).value(), n1.value(), "刷新后不得返回旧值");

        assertEquals(QuerySource.CACHE, n2.source(), "同批未被波及的键应继续命中缓存");
        assertEquals(QuerySource.CACHE, nOther.source(), "其他数据集的键应继续命中缓存");
        assertEquals(first.get(s2).value(), n2.value(), "未被波及键的值不应变化");
        assertEquals(first.get(other).value(), nOther.value(), "无关键的值不应变化");

        assertEquals(6, dependency.invocationCount(), "只有两个被刷新的键允许重算");
    }

    @Test
    void singleRefreshTakesEffectImmediatelyWithinTtl() {
        String key = "refreshed-key";
        QueryResult r1 = q(key);
        assertEquals(QuerySource.COMPUTED, r1.source());
        assertEquals(QuerySource.CACHE, q(key).source(), "刷新前应命中旧缓存");

        facade.refresh(key); // 模拟后台刷新，远未到 TTL

        QueryResult r2 = q(key);
        assertEquals(QuerySource.COMPUTED, r2.source(), "刷新后的第一次查询必须反映新状态");
        assertNotEquals(r1.value(), r2.value(), "仍在 TTL 内也不得返回旧值");
        assertEquals(2, dependency.invocationCount());

        assertEquals(QuerySource.CACHE, q(key).source(), "刷新后新值应被正常缓存复用");
        assertEquals(r2.value(), q(key).value());
        assertEquals(2, dependency.invocationCount(), "新值命中缓存不得再触发计算");
    }

    @Test
    void prefixRefreshOnlyAffectsMatchingKeys() {
        String a0 = "dataset-1:shard-0";
        String a1 = "dataset-1:shard-1";
        String b0 = "dataset-2:shard-0";
        QueryResult v0 = q(a0);
        QueryResult v1 = q(a1);
        QueryResult vb = q(b0);
        assertEquals(3, dependency.invocationCount());

        int affected = facade.refreshByPrefix("dataset-1:");
        log.info("[test] prefix refresh dataset-1: affected={}", affected);
        assertEquals(2, affected);

        assertNotEquals(v0.value(), q(a0).value());
        assertNotEquals(v1.value(), q(a1).value());
        assertEquals(vb.value(), q(b0).value(), "前缀不匹配的键继续命中旧值");
        assertEquals(5, dependency.invocationCount(), "仅前缀匹配的两个键重算");
    }
}
