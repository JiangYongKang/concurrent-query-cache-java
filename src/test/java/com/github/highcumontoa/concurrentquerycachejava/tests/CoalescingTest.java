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
 * 场景一：并发合并 —— 同一键的并发请求只触发一次底层计算。
 */
class CoalescingTest {

    private static final Logger log = LoggerFactory.getLogger(CoalescingTest.class);

    private QueryCacheProperties props;
    private SlowDependency dependency;
    private QueryFacade facade;

    @BeforeEach
    void setUp() {
        props = new QueryCacheProperties();
        props.setSimulatedDelayMillis(150);
        props.setTtlMillis(5_000);
        QueryCacheStore store = new QueryCacheStore(props);
        dependency = new SlowDependency(props);
        facade = new QueryFacade(props, store, new RequestCoalescer(), new ConcurrencyLimiter(props), dependency);
    }

    @Test
    void concurrentRequestsForSameKeyTriggerSingleComputation() throws Exception {
        int threads = 8;
        String key = "hot-key";
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<QueryResult>> futures = IntStream.range(0, threads)
                    .mapToObj(i -> pool.submit(() -> {
                        ready.countDown();
                        go.await();
                        return facade.query(key);
                    }))
                    .toList();
            assertTrue(ready.await(2, TimeUnit.SECONDS));
            go.countDown();

            long computed = 0, coalesced = 0;
            String firstValue = null;
            for (Future<QueryResult> f : futures) {
                QueryResult r = f.get(5, TimeUnit.SECONDS);
                log.info("[test] key={} source={} value={} elapsed={}ms", r.key(), r.source(), r.value(), r.elapsedMillis());
                assertEquals(key, r.key());
                assertFalse(r.degraded());
                if (firstValue == null) {
                    firstValue = r.value();
                }
                assertEquals(firstValue, r.value(), "所有并发请求必须拿到同一个结果");
                if (r.source() == QuerySource.COMPUTED) computed++;
                if (r.source() == QuerySource.COALESCED) coalesced++;
            }
            log.info("[test] key={} computed={} coalesced={} invocations={}",
                    key, computed, coalesced, dependency.invocationCount());
            assertEquals(1, dependency.invocationCount(), "底层计算只能触发一次");
            assertEquals(1, computed, "只有一个请求是计算发起方");
            assertEquals(threads - 1, coalesced, "其余请求均为合并复用");
        } finally {
            pool.shutdownNow();
        }
    }
}
