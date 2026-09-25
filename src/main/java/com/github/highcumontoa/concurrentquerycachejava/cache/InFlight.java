package com.github.highcumontoa.concurrentquerycachejava.cache;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 同一个键当前在途（进行中）的唯一一次计算。
 *
 * <p>第一个请求成为 leader 执行计算，其余请求成为 waiter 合并等待；
 * waiter 数量通过 {@link AtomicInteger} CAS 控制，受单键容量约束（背压）。
 */
final class InFlight<V> {

    private final CompletableFuture<V> future;
    private final AtomicInteger waiters;

    InFlight() {
        this.future = new CompletableFuture<>();
        this.waiters = new AtomicInteger(0);
    }

    CompletableFuture<V> getFuture() {
        return future;
    }

    /**
     * 尝试登记一个合并等待者。
     *
     * @param maxWaiters 单键等待队列容量
     * @return true 表示登记成功；false 表示队列已满，调用方应以 QUEUE_FULL 快速失败
     */
    boolean tryAcquireWaiterSlot(int maxWaiters) {
        while (true) {
            int current = waiters.get();
            if (current >= maxWaiters) {
                return false;
            }
            if (waiters.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    /** 等待者退出（超时/取消/完成后），释放其占用的排队槽位。 */
    void releaseWaiterSlot() {
        waiters.decrementAndGet();
    }

    int waiterCount() {
        return waiters.get();
    }
}
