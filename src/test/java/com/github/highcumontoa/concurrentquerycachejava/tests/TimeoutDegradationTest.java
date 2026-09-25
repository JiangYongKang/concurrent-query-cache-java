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
 * 场景三：超时降级与背压 —— 底层不可用时按开关返回带标记的旧值；
 * 排队超容量或等待超时以可区分原因快速失败。
 */
class TimeoutDegradationTest {

    private static final Logger log = LoggerFactory.getLogger(TimeoutDegradationTest.class);

    private QueryCacheProperties props;
    private SlowDependency dependency;
    private QueryFacade facade;

    @BeforeEach
    void setUp() {
        props = new QueryCacheProperties();
        props.setSimulatedDelayMillis(20);
        props.setTtlMillis(150);
        props.setComputeTimeoutMillis(300);
        props.setStaleFallbackEnabled(true);
        QueryCacheStore store = new QueryCacheStore(props);
        dependency = new SlowDependency(props);
        facade = new QueryFacade(props, store, new RequestCoalescer(), new ConcurrencyLimiter(props), dependency);
    }

    @Test
    void computeTimeoutFallsBackToStaleWithDegradedFlag() throws Exception {
        String key = "degraded-key";
        QueryResult r1 = facade.query(key);
        log.info("[test] key={} source={} value={} elapsed={}ms", key, r1.source(), r1.value(), r1.elapsedMillis());
        assertEquals(QuerySource.COMPUTED, r1.source());

        Thread.sleep(250); // 让条目过期
        dependency.setDelayOverride(2_000L); // 依赖变得极慢，必然触发计算超时

        QueryResult r2 = facade.query(key);
        log.info("[test] key={} source={} value={} degraded={} elapsed={}ms",
                key, r2.source(), r2.value(), r2.degraded(), r2.elapsedMillis());
        assertEquals(QuerySource.STALE_DEGRADED, r2.source(), "超时应降级返回旧值");
        assertTrue(r2.degraded(), "降级结果必须带明确标记");
        assertEquals(r1.value(), r2.value(), "降级应返回旧值");
        assertTrue(r2.elapsedMillis() < 1_000, "降级必须快速返回而不是等慢依赖");
    }

    @Test
    void fallbackDisabledPropagatesDistinguishableFailure() throws Exception {
        String key = "no-fallback-key";
        facade.query(key);
        Thread.sleep(250);
        props.setStaleFallbackEnabled(false);

        // 计算超时：原因 COMPUTE_TIMEOUT
        dependency.setDelayOverride(2_000L);
        QueryFailureException timeout = assertThrows(QueryFailureException.class, () -> facade.query(key));
        log.info("[test] key={} failure reason={} message={}", key, timeout.getReason(), timeout.getMessage());
        assertEquals(QueryFailureException.Reason.COMPUTE_TIMEOUT, timeout.getReason());

        // 依赖故障：原因 UPSTREAM_ERROR
        dependency.setDelayOverride(null);
        dependency.setFailing(true);
        QueryFailureException upstream = assertThrows(QueryFailureException.class, () -> facade.query(key));
        log.info("[test] key={} failure reason={} message={}", key, upstream.getReason(), upstream.getMessage());
        assertEquals(QueryFailureException.Reason.UPSTREAM_ERROR, upstream.getReason());
    }

    @Test
    void backpressureRejectsWithDistinguishableReasons() throws Exception {
        props.setMaxConcurrency(1);
        props.setQueueCapacity(1);
        props.setAcquireTimeoutMillis(100);
        props.setTtlMillis(0); // 禁用缓存，全部直达底层
        QueryCacheStore store = new QueryCacheStore(props);
        SlowDependency dep = new SlowDependency(props);
        dep.setDelayOverride(600L); // 占住唯一许可
        QueryFacade f = new QueryFacade(props, store, new RequestCoalescer(), new ConcurrencyLimiter(props), dep);

        int threads = 6;
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<String>> futures = IntStream.range(0, threads)
                    .mapToObj(i -> pool.submit(() -> {
                        ready.countDown();
                        go.await();
                        String key = "bp-key-" + i;
                        long start = System.nanoTime();
                        try {
                            QueryResult r = f.query(key);
                            return "OK:" + r.source();
                        } catch (QueryFailureException e) {
                            long elapsed = (System.nanoTime() - start) / 1_000_000;
                            log.info("[test] key={} failure reason={} elapsed={}ms", key, e.getReason(), elapsed);
                            return "FAIL:" + e.getReason();
                        }
                    }))
                    .toList();
            assertTrue(ready.await(2, TimeUnit.SECONDS));
            go.countDown();

            long overloads = 0, acquireTimeouts = 0, oks = 0;
            for (Future<String> fu : futures) {
                String outcome = fu.get(10, TimeUnit.SECONDS);
                log.info("[test] outcome={}", outcome);
                switch (outcome) {
                    case "FAIL:OVERLOADED" -> overloads++;
                    case "FAIL:ACQUIRE_TIMEOUT" -> acquireTimeouts++;
                    default -> oks++;
                }
            }
            log.info("[test] ok={} overloaded={} acquireTimeout={} invocations={}",
                    oks, overloads, acquireTimeouts, dep.invocationCount());
            assertTrue(overloads + acquireTimeouts > 0, "背压必须拒绝部分请求");
            assertTrue(dep.invocationCount() <= 2, "并发上限为 1 时底层调用不得失控, actual=" + dep.invocationCount());
        } finally {
            pool.shutdownNow();
        }
    }
}
