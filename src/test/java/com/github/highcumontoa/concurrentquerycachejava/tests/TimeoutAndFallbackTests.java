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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景三：超时、降级、并发上限与背压。
 * 断言失败原因可区分（COMPUTE_TIMEOUT / WAIT_TIMEOUT / QUEUE_FULL / COMPUTE_ERROR），
 * 超时不写缓存；降级开关关闭时失败、开启时返回 degraded=true 的旧值；
 * 底层并发不突破上限；排队超容量快速失败；超时/取消释放许可与排队槽位。
 */
class TimeoutAndFallbackTests {

    private static final Logger log = LoggerFactory.getLogger(TimeoutAndFallbackTests.class);

    private QueryCacheProperties base() {
        QueryCacheProperties p = new QueryCacheProperties();
        p.setTtlMillis(200);
        p.setStaleAcceptedMillis(30_000);
        p.setStaleOnFailureAllowed(false);
        p.setComputeConcurrency(2);
        p.setMaxWaitersPerKey(10);
        p.setWaitTimeoutMillis(3_000);
        p.setComputeTimeoutMillis(2_000);
        return p;
    }

    @Test
    void computeTimeoutIsNotCachedAndReleasesPermit() throws InterruptedException {
        QueryCacheProperties p = base();
        p.setComputeTimeoutMillis(300);
        p.setWaitTimeoutMillis(600);
        QueryCache<String> cache = new QueryCache<>(p);
        AtomicInteger invocations = new AtomicInteger();
        AtomicBoolean interrupted = new AtomicBoolean();
        String key = "k-timeout";

        QueryResult<String> r1 = cache.get(key, () -> {
            invocations.incrementAndGet();
            try {
                Thread.sleep(900);
            } catch (InterruptedException e) {
                interrupted.set(true);
                Thread.currentThread().interrupt();
            }
            return "slow";
        });
        log.info("[TEST] key={} status={} reason={} degraded={} elapsedMs={}",
                key, r1.getStatus(), r1.getReason(), r1.isDegraded(), r1.getElapsedMillis());
        assertEquals(CacheStatus.FAILED, r1.getStatus());
        assertEquals(FailureReason.COMPUTE_TIMEOUT, r1.getReason(), "计算超时原因必须可区分");
        assertEquals(0, cache.cacheSize(), "超时结果不得写入缓存");

        // 超时后被取消的底层任务应收到中断，且计算许可必须归还
        long deadline = System.currentTimeMillis() + 1000;
        while (!interrupted.get() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertTrue(interrupted.get(), "超时必须中断底层慢任务以释放其占用资源");
        assertEquals(p.getComputeConcurrency(), cache.availableComputePermits(),
                "超时后计算许可必须全部释放");

        // 后续请求重新触发计算并成功
        QueryResult<String> r2 = cache.get(key, () -> {
            invocations.incrementAndGet();
            return "fast";
        });
        log.info("[TEST] key={} status={} value={} elapsedMs={}",
                key, r2.getStatus(), r2.getValue(), r2.getElapsedMillis());
        assertEquals(CacheStatus.COMPUTED, r2.getStatus());
        assertEquals("fast", r2.getValue());
        assertEquals(2, invocations.get(), "超时不写缓存，后续请求必须重新触发计算");
        cache.shutdown();
    }

    @Test
    void staleFallbackIsSwitchableAndDistinguishable() throws InterruptedException {
        String key = "k-stale";

        // 1) 先在“允许降级”的缓存里种下一个旧值
        QueryCacheProperties allowed = base();
        allowed.setStaleOnFailureAllowed(true);
        QueryCache<String> cacheAllowed = new QueryCache<>(allowed);
        AtomicInteger callsAllowed = new AtomicInteger();
        QueryResult<String> seed = cacheAllowed.get(key, () -> {
            callsAllowed.incrementAndGet();
            return "v1";
        });
        assertEquals(CacheStatus.COMPUTED, seed.getStatus());
        Thread.sleep(350); // 超过 TTL，条目变为过期旧值

        // 2) 开关关闭：底层错误时必须失败，不得返回旧值
        QueryCacheProperties denied = base();
        denied.setStaleOnFailureAllowed(false);
        QueryCache<String> cacheDenied = new QueryCache<>(denied);
        // 复用同一份条目：在关闭开关的缓存中先种值再过期
        AtomicInteger callsDenied = new AtomicInteger();
        cacheDenied.get(key, () -> {
            callsDenied.incrementAndGet();
            return "v1";
        });
        Thread.sleep(350);
        QueryResult<String> deniedResult = cacheDenied.get(key, () -> {
            callsDenied.incrementAndGet();
            throw new IllegalStateException("backend down");
        });
        log.info("[TEST] key={} fallback=OFF status={} reason={} degraded={} value={} elapsedMs={}",
                key, deniedResult.getStatus(), deniedResult.getReason(),
                deniedResult.isDegraded(), deniedResult.getValue(), deniedResult.getElapsedMillis());
        assertEquals(CacheStatus.FAILED, deniedResult.getStatus());
        assertEquals(FailureReason.COMPUTE_ERROR, deniedResult.getReason());
        assertFalse(deniedResult.isDegraded());

        // 3) 开关开启：底层错误时返回过期旧值，且带明确降级标记
        QueryResult<String> fallback = cacheAllowed.get(key, () -> {
            callsAllowed.incrementAndGet();
            throw new IllegalStateException("backend down");
        });
        log.info("[TEST] key={} fallback=ON status={} reason={} degraded={} value={} elapsedMs={}",
                key, fallback.getStatus(), fallback.getReason(),
                fallback.isDegraded(), fallback.getValue(), fallback.getElapsedMillis());
        assertEquals(CacheStatus.STALE_FALLBACK, fallback.getStatus(), "降级结果状态必须可区分");
        assertTrue(fallback.isDegraded(), "降级结果必须带 degraded=true 标记");
        assertEquals("v1", fallback.getValue());

        // 4) 底层恢复后必须返回新值，降级旧值不再被使用
        QueryResult<String> recovered = cacheAllowed.get(key, () -> {
            callsAllowed.incrementAndGet();
            return "v2";
        });
        assertEquals(CacheStatus.COMPUTED, recovered.getStatus());
        assertEquals("v2", recovered.getValue());
        log.info("[TEST] key={} recovered status={} value={}", key, recovered.getStatus(),
                recovered.getValue());
        cacheAllowed.shutdown();
        cacheDenied.shutdown();
    }

    @Test
    void computeConcurrencyNeverExceedsLimit() throws InterruptedException {
        QueryCacheProperties p = base();
        p.setComputeConcurrency(2);
        p.setWaitTimeoutMillis(5_000);
        QueryCache<String> cache = new QueryCache<>(p);
        AtomicInteger concurrent = new AtomicInteger();
        AtomicInteger maxConcurrent = new AtomicInteger();
        AtomicInteger invocations = new AtomicInteger();
        int keys = 6;
        CyclicBarrier barrier = new CyclicBarrier(keys);

        List<Thread> workers = IntStream.range(0, keys).mapToObj(i -> {
            String key = "k-conc-" + i;
            return new Thread(() -> {
                try {
                    barrier.await();
                } catch (Exception ignored) {
                }
                QueryResult<String> r = cache.get(key, () -> {
                    invocations.incrementAndGet();
                    int now = concurrent.incrementAndGet();
                    maxConcurrent.accumulateAndGet(now, Math::max);
                    try {
                        Thread.sleep(400);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    concurrent.decrementAndGet();
                    return key + "-v";
                });
                log.info("[TEST] key={} status={} coalesced={} reason={} elapsedMs={}",
                        key, r.getStatus(), r.isCoalesced(), r.getReason(), r.getElapsedMillis());
            });
        }).toList();
        workers.forEach(Thread::start);
        for (Thread t : workers) {
            t.join(15_000);
        }
        log.info("[TEST] concurrency limit check: invocations={}, maxConcurrent={}, limit={}",
                invocations.get(), maxConcurrent.get(), p.getComputeConcurrency());
        assertEquals(keys, invocations.get(), "六个不同键都应完成一次计算");
        assertEquals(p.getComputeConcurrency(), maxConcurrent.get(),
                "底层实际并发不得突破配置上限");
        cache.shutdown();
    }

    @Test
    void queueOverflowFailsFastWithQueueFull() throws InterruptedException {
        QueryCacheProperties p = base();
        p.setComputeConcurrency(1);
        p.setMaxWaitersPerKey(2); // 仅允许 2 个合并等待者 + 1 个 leader
        p.setWaitTimeoutMillis(3_000);
        QueryCache<String> cache = new QueryCache<>(p);
        String key = "k-queue";
        int threads = 10;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        List<QueryResult<String>> results = new CopyOnWriteArrayList<>();

        List<Thread> workers = IntStream.range(0, threads).mapToObj(i -> new Thread(() -> {
            try {
                barrier.await();
            } catch (Exception ignored) {
            }
            QueryResult<String> r = cache.get(key, () -> {
                try {
                    Thread.sleep(700);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return "value";
            });
            log.info("[TEST] key={} status={} coalesced={} reason={} elapsedMs={}",
                    key, r.getStatus(), r.isCoalesced(), r.getReason(), r.getElapsedMillis());
            results.add(r);
        }, "burst-" + i)).toList();
        workers.forEach(Thread::start);
        for (Thread t : workers) {
            t.join(15_000);
        }

        long queued = results.stream()
                .filter(r -> r.getReason() == FailureReason.QUEUE_FULL).count();
        long ok = results.stream().filter(QueryResult::isSuccess).count();
        long fastFailMaxElapsed = results.stream()
                .filter(r -> r.getReason() == FailureReason.QUEUE_FULL)
                .mapToLong(QueryResult::getElapsedMillis).max().orElse(-1);
        log.info("[TEST] backpressure: total={}, accepted(ok)={}, queueFull={}, queueFullMaxElapsedMs={}",
                threads, ok, queued, fastFailMaxElapsed);
        assertEquals(threads, results.size());
        assertEquals(3, ok, "leader 与 2 个等待者应被接纳并共享结果");
        assertEquals(7, queued, "超出容量的请求必须以 QUEUE_FULL 失败");
        assertTrue(fastFailMaxElapsed < 200, "背压必须快速失败，不得排队阻塞");
        cache.shutdown();
    }

    @Test
    void waitTimeoutFailsAndReleasesWaiterSlot() throws InterruptedException {
        QueryCacheProperties p = base();
        p.setComputeConcurrency(1);
        p.setMaxWaitersPerKey(1); // 每次只允许 1 个等待者
        p.setWaitTimeoutMillis(150);
        p.setComputeTimeoutMillis(3_000);
        QueryCache<String> cache = new QueryCache<>(p);
        String key = "k-wait";
        AtomicInteger invocations = new AtomicInteger();
        // 用闩锁把 leader 的计算卡在“尚未完成”阶段，便于先制造等待超时，再放行
        java.util.concurrent.CountDownLatch gate = new java.util.concurrent.CountDownLatch(1);

        // leader 开始一次计算：阻塞在闩上，闩放行后立即完成（保证 waiter2 的等待窗口足够）
        Thread leader = new Thread(() -> {
            QueryResult<String> r = cache.get(key, () -> {
                invocations.incrementAndGet();
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return "done";
            });
            log.info("[TEST] key={} leader status={} elapsedMs={}", key, r.getStatus(),
                    r.getElapsedMillis());
        });
        leader.start();
        Thread.sleep(100); // 确保 leader 已进入计算

        // 第一个等待者：150ms 后超时失败（leader 仍继续，未被取消）
        long t0 = System.currentTimeMillis();
        QueryResult<String> timedOut = cache.get(key, () -> {
            invocations.incrementAndGet();
            return "should-not-run";
        });
        long elapsed = System.currentTimeMillis() - t0;
        log.info("[TEST] key={} waiter1 status={} reason={} elapsedMs={}",
                key, timedOut.getStatus(), timedOut.getReason(), elapsed);
        assertEquals(CacheStatus.FAILED, timedOut.getStatus());
        assertEquals(FailureReason.WAIT_TIMEOUT, timedOut.getReason(), "等待超时原因必须可区分");
        assertTrue(elapsed < 400, "等待必须在等待超时附近快速返回");
        assertEquals(1, invocations.get(), "等待者超时不得触发第二次底层计算");

        // 槽位被超时等待者释放：此时新请求必须能入队（否则会立刻 QUEUE_FULL）。
        // 让 waiter2 先以合并者身份入队，再放行 leader，保证它走合并路径而非命中缓存。
        java.util.concurrent.atomic.AtomicReference<QueryResult<String>> waiter2Result =
                new java.util.concurrent.atomic.AtomicReference<>();
        Thread waiter2 = new Thread(() -> waiter2Result.set(cache.get(key, () -> {
            invocations.incrementAndGet();
            return "should-not-run";
        })));
        waiter2.start();
        Thread.sleep(80); // 确保 waiter2 已登记到在途队列
        gate.countDown();
        waiter2.join(2_000);

        QueryResult<String> nextWaiter = waiter2Result.get();
        log.info("[TEST] key={} waiter2 status={} coalesced={} reason={} value={} elapsedMs={}",
                key, nextWaiter.getStatus(), nextWaiter.isCoalesced(),
                nextWaiter.getReason(), nextWaiter.getValue(), nextWaiter.getElapsedMillis());
        assertEquals(CacheStatus.COMPUTED, nextWaiter.getStatus());
        assertTrue(nextWaiter.isCoalesced(), "第二个等待者必须合并复用 leader 的结果");
        assertNotNull(nextWaiter.getValue());

        leader.join(3_000);
        assertEquals(1, invocations.get(), "整轮只有 leader 触发了一次底层计算");
        cache.shutdown();
    }
}
