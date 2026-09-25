package com.github.highcumontoa.concurrentquerycachejava.cache;

/**
 * 一次查询的结果包装。
 *
 * <p>成功时 {@link #getStatus()} 为 HIT / COMPUTED / STALE_FALLBACK，{@link #getValue()} 非空；
 * 失败时状态为 FAILED，原因见 {@link #getReason()}（部分场景也可能直接抛 {@link CacheQueryException}）。
 * coalesced 标记本次请求是否合并复用了同一时刻的其他请求结果。
 */
public final class QueryResult<V> {

    private final V value;
    private final CacheStatus status;
    private final FailureReason reason;
    private final boolean coalesced;
    private final long elapsedMillis;

    private QueryResult(V value, CacheStatus status, FailureReason reason,
                        boolean coalesced, long elapsedMillis) {
        this.value = value;
        this.status = status;
        this.reason = reason;
        this.coalesced = coalesced;
        this.elapsedMillis = elapsedMillis;
    }

    static <V> QueryResult<V> hit(V value, long elapsedMillis) {
        return new QueryResult<>(value, CacheStatus.HIT, null, false, elapsedMillis);
    }

    static <V> QueryResult<V> computed(V value, boolean coalesced, long elapsedMillis) {
        return new QueryResult<>(value, CacheStatus.COMPUTED, null, coalesced, elapsedMillis);
    }

    static <V> QueryResult<V> staleFallback(V value, boolean coalesced, long elapsedMillis) {
        return new QueryResult<>(value, CacheStatus.STALE_FALLBACK, null, coalesced, elapsedMillis);
    }

    static <V> QueryResult<V> failed(FailureReason reason, long elapsedMillis) {
        return new QueryResult<>(null, CacheStatus.FAILED, reason, false, elapsedMillis);
    }

    public boolean isSuccess() {
        return status != CacheStatus.FAILED;
    }

    /** 是否为降级旧值：与正常结果的显式区分入口。 */
    public boolean isDegraded() {
        return status == CacheStatus.STALE_FALLBACK;
    }

    public V getValue() {
        return value;
    }

    public CacheStatus getStatus() {
        return status;
    }

    public FailureReason getReason() {
        return reason;
    }

    public boolean isCoalesced() {
        return coalesced;
    }

    public long getElapsedMillis() {
        return elapsedMillis;
    }
}
