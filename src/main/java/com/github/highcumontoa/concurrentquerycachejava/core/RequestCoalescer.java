package com.github.highcumontoa.concurrentquerycachejava.core;

import com.github.highcumontoa.concurrentquerycachejava.config.QueryCacheProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * 并发合并：同一时刻对同一键的多个请求只触发一次底层计算，
 * 其余请求作为“等待方”复用该结果；计算失败时不留痕迹，后续请求重新触发。
 *
 * 等待方受两个上限保护（等待方本身不占用底层计算许可）：
 * - 等待数量超过 coalesceWaiterCapacity：立即以 COALESCER_OVERLOADED 失败；
 * - 等待时长超过 coalesceWaitTimeoutMillis：以 COALESCE_WAIT_TIMEOUT 失败。
 * 被拒绝/超时的等待方干净退出，不影响发起方的计算，也不影响其他键。
 */
@Component
public class RequestCoalescer {

    private static final Logger log = LoggerFactory.getLogger(RequestCoalescer.class);

    private record InFlight<T>(CompletableFuture<T> future, AtomicInteger waiters) {
    }

    private final QueryCacheProperties properties;
    private final ConcurrentHashMap<String, InFlight<?>> inFlight = new ConcurrentHashMap<>();

    public RequestCoalescer(QueryCacheProperties properties) {
        this.properties = properties;
    }

    /**
     * 对同一 key 的并发调用合并为一次 supplier 执行。
     *
     * @throws QueryFailureException 原因可区分：COALESCER_OVERLOADED / COALESCE_WAIT_TIMEOUT，
     *                               以及被合并计算本身抛出的失败原因
     */
    @SuppressWarnings("unchecked")
    public <T> CoalescedResult<T> coalesce(String key, Supplier<T> supplier) {
        InFlight<T> holder = new InFlight<>(new CompletableFuture<>(), new AtomicInteger(0));
        InFlight<T> existing = (InFlight<T>) inFlight.putIfAbsent(key, holder);
        if (existing != null) {
            return awaitExisting(key, existing);
        }
        // 发起方：执行唯一一次计算，并保证无论成败都移除自己的占位
        try {
            T value = supplier.get();
            holder.future().complete(value);
            return new CoalescedResult<>(value, false);
        } catch (RuntimeException e) {
            holder.future().completeExceptionally(e);
            throw e;
        } finally {
            inFlight.remove(key, holder);
        }
    }

    private <T> CoalescedResult<T> awaitExisting(String key, InFlight<T> holder) {
        int queued = holder.waiters().getAndIncrement();
        if (queued >= properties.getCoalesceWaiterCapacity()) {
            holder.waiters().decrementAndGet();
            log.warn("[coalescer] key={} waiters={} capacity={} -> COALESCER_OVERLOADED",
                    key, queued, properties.getCoalesceWaiterCapacity());
            throw new QueryFailureException(QueryFailureException.Reason.COALESCER_OVERLOADED,
                    "coalesce waiter capacity " + properties.getCoalesceWaiterCapacity()
                            + " exceeded for key=" + key);
        }
        try {
            T value = holder.future().get(properties.getCoalesceWaitTimeoutMillis(), TimeUnit.MILLISECONDS);
            log.info("[coalescer] key={} reused in-flight result waiters={}", key, queued + 1);
            return new CoalescedResult<>(value, true);
        } catch (TimeoutException e) {
            log.warn("[coalescer] key={} wait timeout after {}ms -> COALESCE_WAIT_TIMEOUT",
                    key, properties.getCoalesceWaitTimeoutMillis());
            throw new QueryFailureException(QueryFailureException.Reason.COALESCE_WAIT_TIMEOUT,
                    "timed out waiting " + properties.getCoalesceWaitTimeoutMillis()
                            + "ms for in-flight computation of key=" + key, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QueryFailureException(QueryFailureException.Reason.COALESCE_WAIT_TIMEOUT,
                    "interrupted while waiting for in-flight computation of key=" + key, e);
        } catch (ExecutionException e) {
            throw unwrap(e);
        } finally {
            holder.waiters().decrementAndGet();
        }
    }

    private static RuntimeException unwrap(ExecutionException e) {
        Throwable cause = e.getCause() != null ? e.getCause() : e;
        if (cause instanceof RuntimeException re) {
            return re;
        }
        return new QueryFailureException(QueryFailureException.Reason.UPSTREAM_ERROR,
                "coalesced wait failed: " + cause.getMessage(), cause);
    }

    /** 合并结果：值 + 本次调用是否为复用方。 */
    public record CoalescedResult<T>(T value, boolean coalesced) {
    }
}
