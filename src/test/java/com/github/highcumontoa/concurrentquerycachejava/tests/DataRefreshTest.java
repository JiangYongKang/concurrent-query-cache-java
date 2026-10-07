package com.github.highcumontoa.concurrentquerycachejava.tests;

import com.github.highcumontoa.concurrentquerycachejava.config.QueryCacheProperties;
import com.github.highcumontoa.concurrentquerycachejava.core.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景四：后台数据刷新驱动失效。
 * - 单键 / 批量刷新后，即使仍在 TTL 内，受影响键也必须拿到新状态；
 * - 不在变更批次内的键继续命中缓存，不被重算；
 * - 变更前启动、变更后结束的在途计算结果作废（不返回、不入缓存），首个查询反映新状态；
 * - 已作废旧值不再通过降级路径露面，未受影响键的降级行为保持不变。
 */
class DataRefreshTest {

    private static final Logger log = LoggerFactory.getLogger(DataRefreshTest.class);

    private QueryCacheProperties props;
    private QueryCacheStore store;
    private SlowDependency dependency;
    private QueryFacade facade;

    @BeforeEach
    void setUp() {
        props = new QueryCacheProperties();
        props.setSimulatedDelayMillis(30);
        props.setTtlMillis(60_000); // 默认 TTL 很长：刷新后失效必须与 TTL 无关
        props.setStaleFallbackEnabled(true);
        store = new QueryCacheStore(props);
        dependency = new SlowDependency(props);
        facade = new QueryFacade(props, store, new RequestCoalescer(props), new ConcurrencyLimiter(props), dependency);
    }

    private QueryResult queryLogged(String key) {
        QueryResult r = facade.query(key);
        log.info("[test] key={} source={} value={} degraded={} elapsed={}ms",
                r.key(), r.source(), r.value(), r.degraded(), r.elapsedMillis());
        return r;
    }

    @Test
    void batchRefreshUpdatesAffectedKeysButKeepsOthersCached() {
        QueryResult k1 = queryLogged("ds-k1");
        QueryResult k2 = queryLogged("ds-k2");
        QueryResult k3 = queryLogged("ds-k3");
        assertEquals(3, dependency.invocationCount());

        // TTL 内再次查询全部命中
        assertSame(QuerySource.CACHE, queryLogged("ds-k1").source());
        assertSame(QuerySource.CACHE, queryLogged("ds-k2").source());
        assertSame(QuerySource.CACHE, queryLogged("ds-k3").source());
        assertEquals(3, dependency.invocationCount(), "命中缓存不得重算");

        // 后台批量刷新 k1、k2（同一数据集的两个分片），k3 不在批次内
        facade.refreshBatch(Map.of("ds-k1", "new-k1", "ds-k2", "new-k2"));

        QueryResult r1 = queryLogged("ds-k1");
        QueryResult r2 = queryLogged("ds-k2");
        assertEquals(QuerySource.COMPUTED, r1.source(), "刷新后即使 TTL 内也必须重算拿新值");
        assertEquals("new-k1", r1.value());
        assertEquals(QuerySource.COMPUTED, r2.source());
        assertEquals("new-k2", r2.value());

        QueryResult r3 = queryLogged("ds-k3");
        assertEquals(QuerySource.CACHE, r3.source(), "不在批次内的键必须继续命中缓存");
        assertEquals(k3.value(), r3.value(), "无关键的值不应被改动");
        assertEquals(5, dependency.invocationCount(), "只有被刷新的两个键重算，k3 不重算");

        // 新值随后正常缓存
        assertSame(QuerySource.CACHE, queryLogged("ds-k1").source());
        assertSame(QuerySource.CACHE, queryLogged("ds-k2").source());
        assertEquals(5, dependency.invocationCount());
    }

    @Test
    void singleRefreshTakesEffectImmediatelyWithinTtl() {
        QueryResult first = queryLogged("single-k");
        assertEquals(1, dependency.invocationCount());
        assertSame(QuerySource.CACHE, queryLogged("single-k").source());

        facade.refresh("single-k", "fresh-value");

        QueryResult after = queryLogged("single-k");
        assertEquals(QuerySource.COMPUTED, after.source());
        assertEquals("fresh-value", after.value(), "刷新后的第一次查询必须反映新状态");
        assertEquals(2, dependency.invocationCount());
        assertSame(QuerySource.CACHE, queryLogged("single-k").source(), "新状态随后被缓存");
        assertEquals(2, dependency.invocationCount());
    }

    @Test
    void refreshDuringInflightComputationDiscardsStaleResult() throws Exception {
        dependency.setDelayOverride(500L);
        CountDownLatch started = new CountDownLatch(1);
        dependency.setOnComputeStarted(started::countDown);

        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<QueryResult> leader = pool.submit(() -> facade.query("race-k"));
            assertTrue(started.await(2, TimeUnit.SECONDS), "在途计算应当已经启动");

            // 在途计算持有旧快照期间后台刷新
            facade.refresh("race-k", "FRESH");

            QueryResult r = leader.get(3, TimeUnit.SECONDS);
            log.info("[test] key=race-k source={} value={} elapsed={}ms", r.source(), r.value(), r.elapsedMillis());
            assertEquals("FRESH", r.value(), "变更前启动的旧结果不得返回，发起方必须基于新版本重算");
            assertEquals(2, dependency.invocationCount(), "旧计算作废后应恰好重算一次");

            QueryResult cached = queryLogged("race-k");
            assertEquals(QuerySource.CACHE, cached.source(), "重算的新值必须已入缓存");
            assertEquals("FRESH", cached.value());
            assertEquals(2, dependency.invocationCount(), "旧结果绝不允许成为可命中数据");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void coalescedWaitersAlsoReceiveFreshValueAfterRefresh() throws Exception {
        dependency.setDelayOverride(500L);
        int threads = 4;
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        dependency.setOnComputeStarted(started::countDown);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<QueryResult>> futures = IntStream.range(0, threads)
                    .mapToObj(i -> pool.submit(() -> {
                        ready.countDown();
                        go.await();
                        return facade.query("waiter-k");
                    }))
                    .toList();
            assertTrue(ready.await(2, TimeUnit.SECONDS));
            go.countDown();
            assertTrue(started.await(2, TimeUnit.SECONDS));

            facade.refresh("waiter-k", "FRESH-W");

            long computed = 0, coalesced = 0;
            for (Future<QueryResult> f : futures) {
                QueryResult r = f.get(3, TimeUnit.SECONDS);
                log.info("[test] key=waiter-k source={} value={} elapsed={}ms", r.source(), r.value(), r.elapsedMillis());
                assertEquals("FRESH-W", r.value(), "等待复用方也绝不能拿到被刷新作废的旧结果");
                if (r.source() == QuerySource.COMPUTED) computed++;
                if (r.source() == QuerySource.COALESCED) coalesced++;
            }
            log.info("[test] computed={} coalesced={} invocations={}", computed, coalesced, dependency.invocationCount());
            assertEquals(1, computed);
            assertEquals(threads - 1, coalesced);
            assertEquals(2, dependency.invocationCount(), "只有一次旧计算 + 一次重算");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void refreshedOldValueIsNeverServedThroughDegradationButUnaffectedKeyStillDegrades() throws Exception {
        props.setTtlMillis(100);
        // 重新装配以应用新 TTL
        store = new QueryCacheStore(props);
        QueryFacade f = new QueryFacade(props, store, new RequestCoalescer(props),
                new ConcurrencyLimiter(props), dependency);

        queryLoggedVia(f, "ref-k");
        queryLoggedVia(f, "other-stale-k");
        Thread.sleep(200); // 两条目都过期（但仍保留为可降级旧值）

        // 仅刷新 ref-k：旧值作废（含降级副本），随后底层又故障
        f.refresh("ref-k", "new-ref");
        dependency.setFailing(true);

        QueryFailureException ex = assertThrows(QueryFailureException.class, () -> f.query("ref-k"));
        log.info("[test] key=ref-k failure reason={} message={}", ex.getReason(), ex.getMessage());
        assertEquals(QueryFailureException.Reason.UPSTREAM_ERROR, ex.getReason());
        // 关键断言：被刷新作废的旧值不能再通过 STALE_DEGRADED 露面（上面直接抛异常即证明）

        // 未被刷新的键：过期旧值仍可正常降级返回，语义保持不变
        QueryResult degraded = f.query("other-stale-k");
        log.info("[test] key=other-stale-k source={} value={} degraded={} elapsed={}ms",
                degraded.source(), degraded.value(), degraded.degraded(), degraded.elapsedMillis());
        assertEquals(QuerySource.STALE_DEGRADED, degraded.source());
        assertTrue(degraded.degraded());
    }

    private QueryResult queryLoggedVia(QueryFacade f, String key) {
        QueryResult r = f.query(key);
        log.info("[test] key={} source={} value={} degraded={} elapsed={}ms",
                r.key(), r.source(), r.value(), r.degraded(), r.elapsedMillis());
        return r;
    }
}
