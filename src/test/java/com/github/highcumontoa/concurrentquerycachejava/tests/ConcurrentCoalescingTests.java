package com.github.highcumontoa.concurrentquerycachejava.tests;

import com.github.highcumontoa.concurrentquerycachejava.cache.CacheStatus;
import com.github.highcumontoa.concurrentquerycachejava.cache.FailureReason;
import com.github.highcumontoa.concurrentquerycachejava.cache.QueryCache;
import com.github.highcumontoa.concurrentquerycachejava.cache.QueryResult;
import com.github.highcumontoa.concurrentquerycachejava.config.QueryCacheProperties;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景一：相同查询的并发合并。
 * 断言：同键并发只触发一次底层计算；其余请求标记 coalesced=true；
 * 计算失败不写缓存，后续请求会重新触发计算；新鲜命中不再触发底层计算。
 */
class ConcurrentCoalescingTests {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentCoalescingTests.class);

    private QueryCacheProperties props() {
        QueryCacheProperties p = new QueryCacheProperties();
        p.setTtlMillis(5_000);
        p.setStaleAcceptedMillis(5_000);
        p.setStaleOnFailureAllowed(false);
        p.setComputeConcurrency(4);
        p.setMaxWaitersPerKey(50);
        p.setWaitTimeoutMillis(3_000);
        p.setComputeTimeoutMillis(3_000);
        return p;
    }

    @Test
    void concurrentSameKeyTriggersSingleCompute() throws Exception {
        QueryCache<String> cache = new QueryCache<>(props());
        AtomicInteger invocations = new AtomicInteger();
        int threads = 20;
        String key = "k-coalesce";
        CyclicBarrier barrier = new CyclicBarrier(threads);

        record Outcome(CacheStatus status, boolean coalesced, long elapsed, FailureReason reason) {
        }

        List<Outcome> outcomes = new java.util.concurrent.CopyOnWriteArrayList<>();
        List<Thread> workers = IntStream.range(0, threads).mapToObj(i -> new Thread(() -> {
            try {
                barrier.await();
            } catch (Exception ignored) {
            }
            QueryResult<String> r = cache.get(key, () -> {
                invocations.incrementAndGet();
                try {
                    Thread.sleep(400);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return "value-of-" + key;
            });
            log.info("[TEST] key={} status={} coalesced={} degraded={} reason={} elapsedMs={}",
                    key, r.getStatus(), r.isCoalesced(), r.isDegraded(), r.getReason(),
                    r.getElapsedMillis());
            outcomes.add(new Outcome(r.getStatus(), r.isCoalesced(),
                    r.getElapsedMillis(), r.getReason()));
        }, "client-" + i)).toList();
        workers.forEach(Thread::start);
        for (Thread t : workers) {
            t.join(10_000);
        }

        assertEquals(threads, outcomes.size(), "所有请求都应返回");
        // 核心断言：20 个同时请求，底层只计算 1 次
        assertEquals(1, invocations.get(), "相同键的并发请求必须合并为一次底层计算");
        long leaders = outcomes.stream().filter(o -> !o.coalesced()).count();
        long merged = outcomes.stream().filter(Outcome::coalesced).count();
        assertEquals(1, leaders, "恰好一个 leader 触发计算");
        assertEquals(threads - 1L, merged, "其余请求必须合并复用结果");
        assertTrue(outcomes.stream().allMatch(o -> o.status() == CacheStatus.COMPUTED),
                "所有结果均应为 COMPUTED");
        log.info("[TEST] coalescing verified: invocations={}, leaders={}, mergedWaiters={}",
                invocations.get(), leaders, merged);
        cache.shutdown();
    }

    @Test
    void distinctKeysAreNotCoalesced() {
        QueryCache<String> cache = new QueryCache<>(props());
        AtomicInteger invocations = new AtomicInteger();
        for (String key : List.of("k-a", "k-b")) {
            QueryResult<String> r = cache.get(key, () -> {
                invocations.incrementAndGet();
                return "v-" + key;
            });
            log.info("[TEST] key={} status={} coalesced={} elapsedMs={}",
                    key, r.getStatus(), r.isCoalesced(), r.getElapsedMillis());
        }
        assertEquals(2, invocations.get(), "不同键不应合并");
        cache.shutdown();
    }

    @Test
    void failedComputeIsNotCachedAndNextRequestRetries() {
        QueryCache<String> cache = new QueryCache<>(props());
        AtomicInteger invocations = new AtomicInteger();
        String key = "k-flaky";

        QueryResult<String> first = cache.get(key, () -> {
            invocations.incrementAndGet();
            throw new IllegalStateException("boom");
        });
        log.info("[TEST] key={} status={} reason={} elapsedMs={}",
                key, first.getStatus(), first.getReason(), first.getElapsedMillis());
        assertEquals(CacheStatus.FAILED, first.getStatus());
        assertEquals(FailureReason.COMPUTE_ERROR, first.getReason());

        QueryResult<String> second = cache.get(key, () -> {
            invocations.incrementAndGet();
            return "recovered";
        });
        log.info("[TEST] key={} status={} coalesced={} elapsedMs={}",
                key, second.getStatus(), second.isCoalesced(), second.getElapsedMillis());
        assertEquals(CacheStatus.COMPUTED, second.getStatus());
        assertEquals("recovered", second.getValue());
        assertEquals(2, invocations.get(), "失败不得写入缓存，后续请求必须重新触发计算");

        QueryResult<String> third = cache.get(key, () -> {
            invocations.incrementAndGet();
            return "should-not-happen";
        });
        log.info("[TEST] key={} status={} coalesced={} elapsedMs={}",
                key, third.getStatus(), third.isCoalesced(), third.getElapsedMillis());
        assertEquals(CacheStatus.HIT, third.getStatus());
        assertEquals(2, invocations.get(), "新鲜命中不得再触发底层计算");
        cache.shutdown();
    }
}
