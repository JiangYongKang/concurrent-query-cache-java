package com.github.highcumontoa.concurrentquerycachejava.tests;

import com.github.highcumontoa.concurrentquerycachejava.config.QueryCacheProperties;
import com.github.highcumontoa.concurrentquerycachejava.core.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景五：同键合并等待的容量与等待上限。
 * - 等待方数量超过容量：尽快以 COALESCER_OVERLOADED 失败；
 * - 等待时长超过上限：以 COALESCE_WAIT_TIMEOUT 失败；
 * - 两类原因可区分，且不影响发起方计算与其他键；等待方不占用底层计算许可。
 */
class CoalescerCapacityTest {

    private static final Logger log = LoggerFactory.getLogger(CoalescerCapacityTest.class);

    private QueryCacheProperties props;
    private SlowDependency dependency;
    private QueryFacade facade;

    @BeforeEach
    void setUp() {
        props = new QueryCacheProperties();
        props.setTtlMillis(0); // 禁用缓存，保证同键请求都走合并/计算
        facade = newFacade();
    }

    private QueryFacade newFacade() {
        QueryCacheStore store = new QueryCacheStore(props);
        dependency = new SlowDependency(props);
        return new QueryFacade(props, store, new RequestCoalescer(props), new ConcurrencyLimiter(props), dependency);
    }

    @Test
    void waiterOverflowIsRejectedFastWithDistinguishableReason() throws Exception {
        props.setMaxConcurrency(4);          // 等待方不应占用底层许可
        props.setCoalesceWaiterCapacity(2);  // 至多 2 个等待方（不含发起方）
        props.setComputeTimeoutMillis(5_000);
        QueryFacade f = newFacade();
        dependency.setDelayOverride(600L);

        int threads = 5; // 1 发起方 + 4 等待方，其中 2 个必须被拒
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<String>> futures = IntStream.range(0, threads)
                    .mapToObj(i -> pool.submit(() -> {
                        ready.countDown();
                        go.await();
                        String key = "cap-k";
                        long start = System.nanoTime();
                        try {
                            QueryResult r = f.query(key);
                            return "OK:" + r.source() + ":" + r.value();
                        } catch (QueryFailureException e) {
                            long elapsed = (System.nanoTime() - start) / 1_000_000;
                            log.info("[test] key={} failure reason={} elapsed={}ms", key, e.getReason(), elapsed);
                            return "FAIL:" + e.getReason() + ":" + elapsed;
                        }
                    }))
                    .toList();
            assertTrue(ready.await(2, TimeUnit.SECONDS));
            go.countDown();

            long ok = 0, overloaded = 0;
            long maxRejectElapsed = 0;
            for (Future<String> fu : futures) {
                String outcome = fu.get(5, TimeUnit.SECONDS);
                log.info("[test] outcome={}", outcome);
                if (outcome.startsWith("OK:")) {
                    ok++;
                } else if (outcome.startsWith("FAIL:COALESCER_OVERLOADED:")) {
                    overloaded++;
                    maxRejectElapsed = Math.max(maxRejectElapsed,
                            Long.parseLong(outcome.substring(outcome.lastIndexOf(':') + 1)));
                } else {
                    fail("不应出现其他失败结果: " + outcome);
                }
            }
            log.info("[test] ok={} overloaded={} invocations={}", ok, overloaded, dependency.invocationCount());
            assertEquals(2, overloaded, "超出等待容量的请求必须被拒绝");
            assertEquals(3, ok, "发起方 + 容量内的 2 个等待方应当成功复用同一次计算");
            assertEquals(1, dependency.invocationCount(), "无论成功或被拒，同键只允许一次底层计算");
            assertTrue(maxRejectElapsed < 100, "超容量必须快速拒绝而不是等待, actual=" + maxRejectElapsed + "ms");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void waiterTimeoutFailsWithDistinguishableReason() throws Exception {
        props.setCoalesceWaitTimeoutMillis(100);
        props.setComputeTimeoutMillis(5_000);
        QueryFacade f = newFacade();
        dependency.setDelayOverride(600L);

        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            // 先占住同键在途计算
            Future<QueryResult> leader = pool.submit(() -> f.query("wait-k"));
            Thread.sleep(100); // 确保发起方已经进入计算

            long start = System.nanoTime();
            QueryFailureException ex = assertThrows(QueryFailureException.class, () -> f.query("wait-k"));
            long elapsed = (System.nanoTime() - start) / 1_000_000;
            log.info("[test] key=wait-k failure reason={} elapsed={}ms", ex.getReason(), elapsed);
            assertEquals(QueryFailureException.Reason.COALESCE_WAIT_TIMEOUT, ex.getReason(),
                    "等待超时原因必须可区分");
            assertTrue(elapsed < 300, "必须在等待上限附近快速失败, actual=" + elapsed + "ms");

            // 发起方不受超时等待方影响，最终成功
            QueryResult leaderResult = leader.get(3, TimeUnit.SECONDS);
            log.info("[test] key=wait-k leader source={} value={}", leaderResult.source(), leaderResult.value());
            assertEquals(QuerySource.COMPUTED, leaderResult.source());
            assertEquals(1, dependency.invocationCount(), "超时等待方不得触发额外计算");

            // 其他键不受影响
            QueryResult other = f.query("wait-other-key");
            log.info("[test] key=wait-other-key source={} value={}", other.source(), other.value());
            assertEquals(QuerySource.COMPUTED, other.source());
            assertEquals(2, dependency.invocationCount());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void rejectedAndTimedOutWaitersDoNotConsumeComputePermits() throws Exception {
        props.setMaxConcurrency(1);                 // 只有 1 个底层许可
        props.setQueueCapacity(16);
        props.setAcquireTimeoutMillis(2_000);
        props.setCoalesceWaiterCapacity(1);
        props.setCoalesceWaitTimeoutMillis(80);
        props.setComputeTimeoutMillis(5_000);
        QueryFacade f = newFacade();
        dependency.setDelayOverride(400L);

        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            // 发起方占住唯一许可并在途计算
            Future<QueryResult> leader = pool.submit(() -> f.query("perm-k"));
            Thread.sleep(100);

            // 一个等待方会超容量被拒；另一个等待方会等待超时
            Future<QueryFailureException> rej = pool.submit(() ->
                    assertThrows(QueryFailureException.class, () -> f.query("perm-k")));
            Future<QueryFailureException> to = pool.submit(() ->
                    assertThrows(QueryFailureException.class, () -> f.query("perm-k")));

            QueryFailureException e1 = rej.get(2, TimeUnit.SECONDS);
            QueryFailureException e2 = to.get(2, TimeUnit.SECONDS);
            log.info("[test] key=perm-k reasons={}, {}", e1.getReason(), e2.getReason());
            assertTrue(e1.getReason() == QueryFailureException.Reason.COALESCER_OVERLOADED
                    || e2.getReason() == QueryFailureException.Reason.COALESCER_OVERLOADED);
            assertTrue(e1.getReason() == QueryFailureException.Reason.COALESCE_WAIT_TIMEOUT
                    || e2.getReason() == QueryFailureException.Reason.COALESCE_WAIT_TIMEOUT);

            // 后续查询改成快速计算：若等待方泄漏了唯一许可，这里将排队直到 ACQUIRE_TIMEOUT 而非快速完成
            dependency.setDelayOverride(20L);
            QueryResult leaderResult = leader.get(3, TimeUnit.SECONDS);
            log.info("[test] key=perm-k leader source={} value={}", leaderResult.source(), leaderResult.value());
            QueryResult next = f.query("perm-after-k");
            log.info("[test] key=perm-after-k source={} value={} elapsed={}ms",
                    next.source(), next.value(), next.elapsedMillis());
            assertEquals(QuerySource.COMPUTED, next.source(), "许可必须已被干净释放");
            assertTrue(next.elapsedMillis() < 150, "不应排队等待泄漏的许可, actual=" + next.elapsedMillis() + "ms");
        } finally {
            pool.shutdownNow();
        }
    }
}
