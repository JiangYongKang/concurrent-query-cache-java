package com.github.highcumontoa.concurrentquerycachejava.core;

import com.github.highcumontoa.concurrentquerycachejava.config.QueryCacheProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.*;
import java.util.function.Supplier;

/**
 * 并发上限与背压：用信号量限制底层计算并发数；
 * 排队线程数超过容量立即拒绝，等待许可超时快速失败；
 * 计算本身受超时约束；无论成功、失败、取消都释放许可。
 */
@Component
public class ConcurrencyLimiter {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyLimiter.class);

    private final QueryCacheProperties properties;
    private final Semaphore permits;
    private final ExecutorService executor;

    public ConcurrencyLimiter(QueryCacheProperties properties) {
        this.properties = properties;
        this.permits = new Semaphore(properties.getMaxConcurrency(), true);
        this.executor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "query-compute-worker");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * 在并发上限约束下执行任务。
     *
     * @throws QueryFailureException 原因可区分：OVERLOADED / ACQUIRE_TIMEOUT / COMPUTE_TIMEOUT / UPSTREAM_ERROR
     */
    public <T> T execute(Supplier<T> task) {
        acquirePermit();
        Future<T> future = executor.submit(task::get);
        try {
            return future.get(properties.getComputeTimeoutMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new QueryFailureException(QueryFailureException.Reason.COMPUTE_TIMEOUT,
                    "compute exceeded " + properties.getComputeTimeoutMillis() + "ms", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof QueryFailureException qfe) {
                throw qfe;
            }
            throw new QueryFailureException(QueryFailureException.Reason.UPSTREAM_ERROR,
                    "upstream compute failed: " + cause.getMessage(), cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            future.cancel(true);
            throw new QueryFailureException(QueryFailureException.Reason.UPSTREAM_ERROR,
                    "interrupted while waiting for compute", e);
        } finally {
            permits.release();
        }
    }

    private void acquirePermit() {
        int waiting = permits.getQueueLength();
        if (waiting >= properties.getQueueCapacity()) {
            log.warn("[limiter] queue full waiting={} capacity={} -> OVERLOADED", waiting, properties.getQueueCapacity());
            throw new QueryFailureException(QueryFailureException.Reason.OVERLOADED,
                    "queue capacity " + properties.getQueueCapacity() + " exceeded");
        }
        try {
            boolean acquired = permits.tryAcquire(properties.getAcquireTimeoutMillis(), TimeUnit.MILLISECONDS);
            if (!acquired) {
                log.warn("[limiter] acquire timeout after {}ms -> ACQUIRE_TIMEOUT", properties.getAcquireTimeoutMillis());
                throw new QueryFailureException(QueryFailureException.Reason.ACQUIRE_TIMEOUT,
                        "timed out waiting " + properties.getAcquireTimeoutMillis() + "ms for a compute permit");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QueryFailureException(QueryFailureException.Reason.ACQUIRE_TIMEOUT,
                    "interrupted while waiting for a compute permit", e);
        }
    }
}
