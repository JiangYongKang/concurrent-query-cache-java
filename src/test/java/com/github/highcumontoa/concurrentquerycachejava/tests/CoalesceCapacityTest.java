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
 * 场景六：同键合并等待的容量与等待上限 ——
 * 等待复用者超过容量立即以 WAIT_OVERLOADED 快速拒绝；
 * 等待超过上限以 WAIT_TIMEOUT 失败；两者原因可区分；
 * 等待者不触发底层计算、不影响在途计算与其他键。
 */
class CoalesceCapacityTest {

    private static final Logger log = LoggerFactory.getLogger(CoalesceCapacityTest.class);

    private QueryCacheProperties props;

    @BeforeEach
    void setUp() {
        props = new QueryCacheProperties();
        props.setSimulatedDelayMillis(20);
        props.setTtlMillis(60_000);
    }

    private record Outcome(String state, QueryFailureException.Reason reason, long elapsedMillis) {
        static Outcome ok(long elapsed) {
            return new Outcome("OK", null, elapsed);
        }
        static Outcome fail(QueryFailureException.Reason reason, long elapsed) {
            return new Outcome("FAIL:" + reason, reason, elapsed);
        }
    }

    private void logOutcome(String key, Outcome o) {
        log.info("[test] key={} outcome={} elapsed={}ms", key, o.state(), o.elapsedMillis());
    }

    @Test
    void exceedingWaitCapacityIsFastRejectedWithoutAffectingOthers() throws Exception {
        props.setCoalesceWaitCapacity(2);   // 最多 2 个等待者
        QueryCacheStore store = new QueryCacheStore(props);
        SlowDependency dep = new SlowDependency(props);
        QueryFacade f = new QueryFacade(props, store, new RequestCoalescer(),
                new ConcurrencyLimiter(props), dep);

        String key = "crowded-key";
        CountDownLatch computeStarted = new CountDownLatch(1);
        CountDownLatch allowFinish = new CountDownLatch(1);
        dep.setGate(computeStarted, allowFinish); // 唯一一次计算挂起

        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            Future<QueryResult> leader = pool.submit(() -> f.query(key));
            assertTrue(computeStarted.await(2, TimeUnit.SECONDS), "发起方计算必须已启动");

            int waiters = 5;
            List<Future<Outcome>> futures = new ArrayList<>();
            for (int i = 0; i < waiters; i++) {
                futures.add(pool.submit(() -> {
                    long s = System.nanoTime();
                    try {
                        QueryResult r = f.query(key);
                        return Outcome.ok((System.nanoTime() - s) / 1_000_000);
                    } catch (QueryFailureException e) {
                        return Outcome.fail(e.getReason(), (System.nanoTime() - s) / 1_000_000);
                    }
                }));
            }

            // 在发起方仍被挂住的窗口期内收集已快速失败的请求（每个 future 只取一次）
            List<Future<Outcome>> pending = new ArrayList<>(futures);
            List<Outcome> rejected = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 1_500;
            while (!pending.isEmpty() && rejected.size() < waiters && System.currentTimeMillis() < deadline) {
                List<Future<Outcome>> stillPending = new ArrayList<>();
                for (Future<Outcome> fu : pending) {
                    if (fu.isDone()) {
                        Outcome o = fu.get(200, TimeUnit.MILLISECONDS);
                        if (o.reason() != null) {
                            rejected.add(o);
                            logOutcome(key, o);
                        }
                        // 已完成但成功的（被接纳的等待者在闸门放行前不会完成），暂不处理
                    } else {
                        stillPending.add(fu);
                    }
                }
                pending = stillPending;
                if (rejected.size() < waiters) {
                    Thread.sleep(10);
                }
            }

            assertEquals(3, rejected.size(), "容量为 2 时应有 3 个等待者被快速拒绝");
            for (Outcome o : rejected) {
                assertEquals(QueryFailureException.Reason.WAIT_OVERLOADED, o.reason(),
                        "超容量的失败原因必须是 WAIT_OVERLOADED");
                assertTrue(o.elapsedMillis() < 500, "超容量必须快速失败，actual=" + o.elapsedMillis() + "ms");
            }

            // 放行在途计算前，另一个键不受同键容量影响（闸门移除后它能正常计算）
            dep.setGate(null, null);
            QueryResult other = f.query("totally-different-key");
            log.info("[test] key=totally-different-key source={} value={} elapsed={}ms",
                    other.source(), other.value(), other.elapsedMillis());
            assertEquals(QuerySource.COMPUTED, other.source(), "其他键不得被同键背压波及");

            allowFinish.countDown(); // 放行在途计算
            QueryResult leaderResult = leader.get(5, TimeUnit.SECONDS);
            log.info("[test] leader key={} source={} value={}", key, leaderResult.source(), leaderResult.value());

            long coalesced = 0;
            for (Future<Outcome> fu : futures) {
                Outcome o = fu.get(5, TimeUnit.SECONDS);
                logOutcome(key, o);
                if (o.reason() == null) {
                    coalesced++;
                }
            }
            assertEquals(2, coalesced, "恰好容量数的等待者被接纳并复用结果");
            assertEquals(2, dep.invocationCount(),
                    "被挤键只计算 1 次 + 其他键 1 次；等待者与被拒者都不得触发计算，actual=" + dep.invocationCount());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void waitingTooLongFailsWithWaitTimeoutAndDoesNotDisturbInFlight() throws Exception {
        props.setCoalesceWaitCapacity(10);
        props.setCoalesceWaitTimeoutMillis(200);
        QueryCacheStore store = new QueryCacheStore(props);
        SlowDependency dep = new SlowDependency(props);
        dep.setDelayOverride(1_500L); // 发起方计算很慢，但不超过 computeTimeout
        QueryFacade f = new QueryFacade(props, store, new RequestCoalescer(),
                new ConcurrencyLimiter(props), dep);

        String key = "slow-key";
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<QueryResult> leader = pool.submit(() -> f.query(key));
            Thread.sleep(100); // 确保发起方已在途

            long start = System.nanoTime();
            Future<QueryFailureException> waiterFailure = pool.submit(() -> {
                try {
                    f.query(key);
                    return null;
                } catch (QueryFailureException e) {
                    return e;
                }
            });

            QueryFailureException e = waiterFailure.get(2, TimeUnit.SECONDS);
            long elapsed = (System.nanoTime() - start) / 1_000_000;
            assertNotNull(e, "等待超时必须抛出失败");
            log.info("[test] key={} failure reason={} elapsed={}ms", key, e.getReason(), elapsed);
            assertEquals(QueryFailureException.Reason.WAIT_TIMEOUT, e.getReason(),
                    "等待超时的原因必须是 WAIT_TIMEOUT，且与 WAIT_OVERLOADED 可区分");
            assertTrue(elapsed < 800, "等待超时应在超时点附近快速失败，actual=" + elapsed + "ms");

            // 在途计算不受影响，正常完成并缓存
            QueryResult leaderResult = leader.get(5, TimeUnit.SECONDS);
            log.info("[test] leader key={} source={} value={}", key, leaderResult.source(), leaderResult.value());
            assertEquals(QuerySource.COMPUTED, leaderResult.source());
            assertEquals(1, dep.invocationCount(), "超时退出的等待者不得触发额外计算");

            QueryResult after = f.query(key);
            log.info("[test] after key={} source={} value={}", key, after.source(), after.value());
            assertEquals(QuerySource.CACHE, after.source(), "在途计算完成后结果应正常可复用");
            assertEquals(leaderResult.value(), after.value());
            assertEquals(1, dep.invocationCount());
        } finally {
            pool.shutdownNow();
        }
    }
}
