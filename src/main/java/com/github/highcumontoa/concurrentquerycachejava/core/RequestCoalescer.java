package com.github.highcumontoa.concurrentquerycachejava.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * 并发合并：同一时刻对同一键的多个请求只触发一次底层计算，
 * 其余请求等待并复用该结果；计算失败时不留痕迹，后续请求重新触发。
 *
 * <p>等待复用受两道约束：等待者数量超过 {@code waitCapacity} 立即以
 * {@link QueryFailureException.Reason#WAIT_OVERLOADED} 快速拒绝；
 * 等待时长超过 {@code waitTimeoutMillis} 以
 * {@link QueryFailureException.Reason#WAIT_TIMEOUT} 失败。
 * 等待者自身从不触发底层计算，因此天然不会绕过并发上限；
 * 被拒绝或超时的等待者只移除自己的计数，不影响在途计算与其他键。
 */
@Component
public class RequestCoalescer {

    private static final Logger log = LoggerFactory.getLogger(RequestCoalescer.class);

    /** 单键在途计算占位：结果 future + 当前等待复用者计数。 */
    private static final class InFlight {
        private final CompletableFuture<Object> future = new CompletableFuture<>();
        private final AtomicInteger waiters = new AtomicInteger(0);
    }

    private final ConcurrentHashMap<String, InFlight> inFlight = new ConcurrentHashMap<>();

    /** 不限容量/超时的兼容入口。 */
    public <T> CoalescedResult<T> coalesce(String key, Supplier<T> supplier) {
        return coalesce(key, Integer.MAX_VALUE, Long.MAX_VALUE, supplier);
    }

    /**
     * 对同一 key 的并发调用合并为一次 supplier 执行。
     *
     * @param waitCapacity       该键允许同时等待复用结果的最大请求数（不含发起方）
     * @param waitTimeoutMillis  等待复用结果的最大时长
     */
    @SuppressWarnings("unchecked")
    public <T> CoalescedResult<T> coalesce(String key, int waitCapacity, long waitTimeoutMillis,
                                           Supplier<T> supplier) {
        InFlight created = new InFlight();
        InFlight existing = inFlight.putIfAbsent(key, created);
        if (existing != null) {
            return waitFor(key, existing, waitCapacity, waitTimeoutMillis);
        }
        // 发起方：执行唯一一次计算，并保证无论成败都移除占位
        try {
            T value = supplier.get();
            created.future.complete(value);
            return new CoalescedResult<>(value, false);
        } catch (RuntimeException e) {
            created.future.completeExceptionally(e);
            throw e;
        } finally {
            inFlight.remove(key, created);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> CoalescedResult<T> waitFor(String key, InFlight flight, int waitCapacity,
                                           long waitTimeoutMillis) {
        // 容量闸门：CAS 计数，超出立即拒绝；绝不触碰在途计算
        while (true) {
            int current = flight.waiters.get();
            if (current >= waitCapacity) {
                log.warn("[coalescer] key={} waiters={} capacity={} -> WAIT_OVERLOADED",
                        key, current, waitCapacity);
                throw new QueryFailureException(QueryFailureException.Reason.WAIT_OVERLOADED,
                        "coalesce wait capacity " + waitCapacity + " exceeded for key=" + key);
            }
            if (flight.waiters.compareAndSet(current, current + 1)) {
                break;
            }
        }
        try {
            T value = (T) flight.future.get(waitTimeoutMillis, TimeUnit.MILLISECONDS);
            log.info("[coalescer] key={} reused in-flight result", key);
            return new CoalescedResult<>(value, true);
        } catch (TimeoutException e) {
            log.warn("[coalescer] key={} wait timeout after {}ms -> WAIT_TIMEOUT", key, waitTimeoutMillis);
            throw new QueryFailureException(QueryFailureException.Reason.WAIT_TIMEOUT,
                    "timed out waiting " + waitTimeoutMillis + "ms for in-flight result of key=" + key, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QueryFailureException(QueryFailureException.Reason.WAIT_TIMEOUT,
                    "interrupted while waiting for in-flight result of key=" + key, e);
        } catch (Exception e) {
            throw unwrap(e);
        } finally {
            flight.waiters.decrementAndGet();
        }
    }

    private static RuntimeException unwrap(Exception e) {
        Throwable cause = e instanceof java.util.concurrent.CompletionException && e.getCause() != null
                ? e.getCause() : e;
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
