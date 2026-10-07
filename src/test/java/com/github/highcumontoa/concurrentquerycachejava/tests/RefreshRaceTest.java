package com.github.highcumontoa.concurrentquerycachejava.tests;

import com.github.highcumontoa.concurrentquerycachejava.config.QueryCacheProperties;
import com.github.highcumontoa.concurrentquerycachejava.core.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景五：数据刷新与在途计算交错 ——
 * "变更前启动、变更后才结束"的计算结果既不能对外返回，也不能写入缓存；
 * 发起方必须在新世代重算，合并等待者也只能拿到新状态；
 * 变更后作废的旧值不允许再通过降级路径露面。
 */
class RefreshRaceTest {

    private static final Logger log = LoggerFactory.getLogger(RefreshRaceTest.class);

    private QueryCacheProperties props;
    private SlowDependency dependency;
    private QueryFacade facade;

    @BeforeEach
    void setUp() {
        props = new QueryCacheProperties();
        props.setSimulatedDelayMillis(20);
        props.setTtlMillis(60_000);
        QueryCacheStore store = new QueryCacheStore(props);
        dependency = new SlowDependency(props);
        facade = new QueryFacade(props, store, new RequestCoalescer(), new ConcurrencyLimiter(props), dependency);
    }

    @Test
    void staleInFlightComputeIsDiscardedAndRecomputedAfterRefresh() throws Exception {
        String key = "race-key";
        CountDownLatch computeStarted = new CountDownLatch(1);
        CountDownLatch allowFinish = new CountDownLatch(1);
        dependency.setGate(computeStarted, allowFinish); // 第一次计算启动后挂起

        ExecutorService pool = Executors.newFixedThreadPool(3);
        try {
            // 发起方：计算#1 在途
            Future<QueryResult> leader = pool.submit(() -> timed(key));
            assertTrue(computeStarted.await(2, TimeUnit.SECONDS), "第一次计算必须已经启动");

            // 两个合并等待者在刷新前加入，它们在等待在途结果
            List<Future<QueryResult>> waiters = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                waiters.add(pool.submit(() -> timed(key)));
            }
            Thread.sleep(100); // 等待待者注册到合并队列（发起方仍被闸门挂住）

            dependency.setGate(null, null); // 只让第一次计算走闸门，重算不再阻塞
            facade.refresh(key);           // 变更发生：推进世代、清除旧值
            log.info("[test] key={} data refreshed while compute#1 in flight", key);
            allowFinish.countDown();       // 放行，计算#1 在刷新之后才结束

            QueryResult leaderResult = leader.get(5, TimeUnit.SECONDS);
            log.info("[test] leader key={} source={} value={} elapsed={}ms",
                    key, leaderResult.source(), leaderResult.value(), leaderResult.elapsedMillis());
            assertEquals(QuerySource.COMPUTED, leaderResult.source());
            assertTrue(leaderResult.value().endsWith("-2"),
                    "变更前启动的旧结果必须作废，发起方应拿到重算的新值，actual=" + leaderResult.value());

            long coalesced = 0;
            for (Future<QueryResult> f : waiters) {
                QueryResult r = f.get(5, TimeUnit.SECONDS);
                log.info("[test] waiter key={} source={} value={} elapsed={}ms",
                        key, r.source(), r.value(), r.elapsedMillis());
                assertEquals(leaderResult.value(), r.value(), "合并等待者也只能拿到刷新后的新值");
                if (r.source() == QuerySource.COALESCED) {
                    coalesced++;
                }
            }
            assertEquals(2, coalesced, "两个等待者都应是合并复用，且复用的是新结果");
            assertEquals(2, dependency.invocationCount(),
                    "只有发起方重算一次；等待者不得触发额外底层计算");

            // 旧结果绝不能被缓存：后续查询命中的必须是新值
            QueryResult cached = facade.query(key);
            log.info("[test] after key={} source={} value={}", key, cached.source(), cached.value());
            assertEquals(QuerySource.CACHE, cached.source());
            assertEquals(leaderResult.value(), cached.value(), "缓存中不得残留作废的旧结果");
            assertEquals(2, dependency.invocationCount(), "命中新缓存不得再计算");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void refreshedStaleValueMustNotReappearThroughDegradation() throws Exception {
        String key = "degraded-race-key";
        props.setTtlMillis(150);
        props.setStaleFallbackEnabled(true);

        QueryResult first = timed(key); // 计算#1，缓存 value-1
        assertEquals(1, dependency.invocationCount());
        Thread.sleep(250); // TTL 过期，但旧值仍可作为降级副本

        CountDownLatch computeStarted = new CountDownLatch(1);
        CountDownLatch allowFinish = new CountDownLatch(1);
        dependency.setGate(computeStarted, allowFinish);

        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            // 计算#2 在途（过期后触发的重算）
            Future<QueryResult> pending = pool.submit(() -> timed(key));
            assertTrue(computeStarted.await(2, TimeUnit.SECONDS));

            dependency.setFailing(true); // 放行后让这次计算失败
            facade.refresh(key);         // 变更：旧值副本与降级资格一并清除，世代推进
            log.info("[test] key={} refreshed while failing compute#2 in flight", key);
            allowFinish.countDown();

            ExecutionException ee = assertThrows(ExecutionException.class,
                    () -> pending.get(5, TimeUnit.SECONDS));
            assertTrue(ee.getCause() instanceof QueryFailureException,
                    "底层失败应原样传播，actual=" + ee.getCause());
            QueryFailureException qfe = (QueryFailureException) ee.getCause();
            log.info("[test] key={} failure reason={} (旧值不得降级返回)", key, qfe.getReason());
            assertEquals(QueryFailureException.Reason.UPSTREAM_ERROR, qfe.getReason());

            // 依赖恢复后的第一次查询必须是新状态，绝不能是旧值 value-1
            dependency.setFailing(false);
            QueryResult recovered = timed(key);
            log.info("[test] recovered key={} source={} value={} elapsed={}ms",
                    key, recovered.source(), recovered.value(), recovered.elapsedMillis());
            assertEquals(QuerySource.COMPUTED, recovered.source());
            assertNotEquals(first.value(), recovered.value(), "作废旧值不允许通过任何路径重新露面");
            assertTrue(recovered.value().endsWith("-3"), "应是计算#3 产生的新值，actual=" + recovered.value());
            assertEquals(3, dependency.invocationCount());
        } finally {
            pool.shutdownNow();
        }
    }

    private QueryResult timed(String key) {
        long start = System.nanoTime();
        QueryResult r = facade.query(key);
        long elapsed = (System.nanoTime() - start) / 1_000_000;
        log.info("[test] key={} source={} value={} degraded={} elapsed={}ms",
                r.key(), r.source(), r.value(), r.degraded(), elapsed);
        return r;
    }
}
