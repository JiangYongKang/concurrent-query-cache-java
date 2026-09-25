package com.github.highcumontoa.concurrentquerycachejava.cache;

import com.github.highcumontoa.concurrentquerycachejava.config.QueryCacheProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * 面向高并发读取的本地查询加速层。
 *
 * <h3>并发合并（single-flight）</h3>
 * 同一时刻对同一键的多个请求只会触发一次底层计算：第一个请求（leader）执行计算，
 * 其余请求（waiter）登记到 {@link InFlight} 并等待同一个结果。
 *
 * <h3>失败/超时不写缓存</h3>
 * 仅当底层计算成功完成时才写入缓存；计算异常或计算超时一律不写，后续请求会重新触发计算。
 *
 * <h3>有效期与失效</h3>
 * 条目过期后不再作为正常结果返回，而是重新计算；显式 {@link #invalidate(String)} 立即移除。
 *
 * <h3>降级</h3>
 * 重新计算失败/超时，且存在落在降级宽限窗口内的旧值，并且开关允许时，返回 STALE_FALLBACK，
 * 与正常结果明确可区分；否则以对应 {@link FailureReason} 失败。
 *
 * <h3>并发上限与背压</h3>
 * 全局信号量约束底层计算并发；单键等待队列超出容量立即 QUEUE_FULL；
 * 等待许可/结果超时为 WAIT_TIMEOUT；任何超时/取消路径都会释放其占用的排队槽位与计算许可，
 * 并中断仍在运行的底层慢任务。
 */
public class QueryCache<V> {

    private static final Logger log = LoggerFactory.getLogger(QueryCache.class);

    private final ConcurrentHashMap<String, CacheEntry<V>> cache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, InFlight<V>> inFlight = new ConcurrentHashMap<>();
    private final Semaphore computePermits;
    private final ExecutorService computeExecutor;
    private final QueryCacheProperties props;

    public QueryCache(QueryCacheProperties props) {
        this.props = props;
        this.computePermits = new Semaphore(props.getComputeConcurrency(), true);
        // 专用守护线程池承载底层计算：实际并发由信号量约束，超时/取消时可中断释放资源
        ThreadFactory tf = r -> {
            Thread t = new Thread(r, "query-cache-compute");
            t.setDaemon(true);
            return t;
        };
        this.computeExecutor = Executors.newCachedThreadPool(tf);
    }

    /** 关闭承载底层计算的线程池（Spring 销毁时调用）。 */
    public void shutdown() {
        computeExecutor.shutdownNow();
    }

    /**
     * 查询一个键。
     *
     * @param key    查询键
     * @param loader 底层计算（仅在需要时被调用，且同键同一时刻最多一个在执行）
     */
    public QueryResult<V> get(String key, Supplier<V> loader) {
        long start = System.currentTimeMillis();
        long now = start;

        CacheEntry<V> entry = cache.get(key);
        if (entry != null && entry.isFresh(now)) {
            QueryResult<V> r = QueryResult.hit(entry.getValue(), elapsed(start));
            log.info("key={} result=HIT coalesced=false elapsedMs={}", key, r.getElapsedMillis());
            return r;
        }

        // 成为在途计算的 leader，或作为 waiter 合并；登记失败说明排队积压超容量
        InFlight<V> slot = new InFlight<>();
        InFlight<V> existing = inFlight.putIfAbsent(key, slot);
        if (existing == null) {
            return computeAsLeader(key, loader, slot, entry, start);
        }
        return joinAsWaiter(key, existing, entry, start);
    }

    /** leader：获取全局计算许可，执行同一时刻唯一一次底层计算（不占用 waiter 队列槽位）。 */
    private QueryResult<V> computeAsLeader(String key, Supplier<V> loader, InFlight<V> slot,
                                           CacheEntry<V> staleCandidate, long start) {
        boolean permitAcquired;
        try {
            permitAcquired = computePermits.tryAcquire(
                    props.getWaitTimeoutMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            inFlight.remove(key, slot);
            throw new CacheQueryException(FailureReason.CANCELLED,
                    "leader interrupted while waiting compute permit: " + key, e);
        }
        if (!permitAcquired) {
            inFlight.remove(key, slot);
            return failOrStale(key, FailureReason.WAIT_TIMEOUT, staleCandidate, start);
        }

        try {
            // 直接持有底层 Future：超时/取消时可对运行中的慢任务发出中断，真正释放其资源
            Future<V> work = computeExecutor.submit(loader::get);
            V value;
            try {
                value = work.get(props.getComputeTimeoutMillis(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                work.cancel(true);
                slot.getFuture().completeExceptionally(
                        new CacheQueryException(FailureReason.COMPUTE_TIMEOUT,
                                "compute timeout for key: " + key, e));
                // 超时不写缓存
                return failOrStale(key, FailureReason.COMPUTE_TIMEOUT, staleCandidate, start);
            } catch (CancellationException e) {
                slot.getFuture().completeExceptionally(
                        new CacheQueryException(FailureReason.CANCELLED,
                                "compute cancelled for key: " + key, e));
                return failOrStale(key, FailureReason.CANCELLED, staleCandidate, start);
            } catch (java.util.concurrent.ExecutionException e) {
                Throwable cause = e.getCause() == null ? e : e.getCause();
                slot.getFuture().completeExceptionally(
                        new CacheQueryException(FailureReason.COMPUTE_ERROR,
                                "compute error for key: " + key, cause));
                // 计算失败不写缓存
                return failOrStale(key, FailureReason.COMPUTE_ERROR, staleCandidate, start);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                work.cancel(true);
                slot.getFuture().completeExceptionally(
                        new CacheQueryException(FailureReason.CANCELLED,
                                "leader interrupted for key: " + key, e));
                throw new CacheQueryException(FailureReason.CANCELLED,
                        "leader interrupted for key: " + key, e);
            }

            // 仅成功才写缓存
            long now = System.currentTimeMillis();
            cache.put(key, new CacheEntry<>(value, now,
                    props.getTtlMillis(), props.getStaleAcceptedMillis()));
            slot.getFuture().complete(value);
            QueryResult<V> r = QueryResult.computed(value, false, elapsed(start));
            log.info("key={} result=COMPUTED coalesced=false elapsedMs={}", key, r.getElapsedMillis());
            return r;
        } finally {
            computePermits.release();
            inFlight.remove(key, slot);
        }
    }

    /** waiter：在单键队列中登记一个槽位，然后等待 leader 的同一个结果。 */
    private QueryResult<V> joinAsWaiter(String key, InFlight<V> slot,
                                        CacheEntry<V> staleCandidate, long start) {
        if (!slot.tryAcquireWaiterSlot(props.getMaxWaitersPerKey())) {
            return fail(key, FailureReason.QUEUE_FULL, start);
        }
        try {
            V value;
            try {
                value = slot.getFuture().get(props.getWaitTimeoutMillis(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                return failOrStale(key, FailureReason.WAIT_TIMEOUT, staleCandidate, start);
            } catch (CancellationException e) {
                return failOrStale(key, FailureReason.CANCELLED, staleCandidate, start);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause() == null ? e : e.getCause();
                FailureReason reason = cause instanceof CacheQueryException cqe
                        ? cqe.getReason() : FailureReason.COMPUTE_ERROR;
                return failOrStale(key, reason, staleCandidate, start);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CacheQueryException(FailureReason.CANCELLED,
                        "waiter interrupted for key: " + key, e);
            }
            QueryResult<V> r = QueryResult.computed(value, true, elapsed(start));
            log.info("key={} result=COMPUTED coalesced=true elapsedMs={}", key, r.getElapsedMillis());
            return r;
        } finally {
            slot.releaseWaiterSlot();
        }
    }

    /**
     * 计算失败后的统一出口：开关允许且旧值仍在降级宽限窗口内时返回带标记的旧值，
     * 否则返回可区分原因的失败结果。
     */
    private QueryResult<V> failOrStale(String key, FailureReason reason,
                                       CacheEntry<V> staleCandidate, long start) {
        long now = System.currentTimeMillis();
        if (props.isStaleOnFailureAllowed()
                && staleCandidate != null && staleCandidate.isStaleUsable(now)) {
            QueryResult<V> r = QueryResult.staleFallback(staleCandidate.getValue(),
                    false, elapsed(start));
            log.warn("key={} result=STALE_FALLBACK reason={} elapsedMs={}",
                    key, reason, r.getElapsedMillis());
            return r;
        }
        return fail(key, reason, start);
    }

    private QueryResult<V> fail(String key, FailureReason reason, long start) {
        QueryResult<V> r = QueryResult.failed(reason, elapsed(start));
        log.warn("key={} result=FAILED reason={} elapsedMs={}", key, reason, r.getElapsedMillis());
        return r;
    }

    private static long elapsed(long startMillis) {
        return System.currentTimeMillis() - startMillis;
    }

    /** 显式失效单个键；正在进行的计算不受影响，但其成功结果会重新写入。 */
    public void invalidate(String key) {
        cache.remove(key);
    }

    /** 显式失效全部键。 */
    public void invalidateAll() {
        cache.clear();
    }

    /** 仅当键当前新鲜时返回其缓存值，不触发计算（供管理面观测/状态校验使用）。 */
    public Optional<QueryResult<V>> getIfFresh(String key) {
        CacheEntry<V> entry = cache.get(key);
        if (entry != null && entry.isFresh(System.currentTimeMillis())) {
            return Optional.of(QueryResult.hit(entry.getValue(), 0));
        }
        return Optional.empty();
    }

    public int cacheSize() {
        return cache.size();
    }

    /** 当前正在进行底层计算的键数量（测试与监控用）。 */
    public int inFlightCount() {
        return inFlight.size();
    }

    /** 当前剩余的底层计算许可数（测试与监控用）。 */
    public int availableComputePermits() {
        return computePermits.availablePermits();
    }
}
