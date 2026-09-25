package com.github.highcumontoa.concurrentquerycachejava.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * 并发合并：同一时刻对同一键的多个请求只触发一次底层计算，
 * 其余请求等待并复用该结果；计算失败时不留痕迹，后续请求重新触发。
 */
@Component
public class RequestCoalescer {

    private static final Logger log = LoggerFactory.getLogger(RequestCoalescer.class);

    private final ConcurrentHashMap<String, CompletableFuture<?>> inFlight = new ConcurrentHashMap<>();

    /**
     * 对同一 key 的并发调用合并为一次 supplier 执行。
     */
    @SuppressWarnings("unchecked")
    public <T> CoalescedResult<T> coalesce(String key, Supplier<T> supplier) {
        CompletableFuture<T> future = new CompletableFuture<>();
        CompletableFuture<T> existing = (CompletableFuture<T>) inFlight.putIfAbsent(key, future);
        if (existing != null) {
            // 复用方：等待发起方的计算结果
            try {
                T value = existing.join();
                log.info("[coalescer] key={} reused in-flight result", key);
                return new CoalescedResult<>(value, true);
            } catch (Exception e) {
                throw unwrap(e);
            }
        }
        // 发起方：执行唯一一次计算，并保证无论成败都移除占位
        try {
            T value = supplier.get();
            future.complete(value);
            return new CoalescedResult<>(value, false);
        } catch (RuntimeException e) {
            future.completeExceptionally(e);
            throw e;
        } finally {
            inFlight.remove(key, future);
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
