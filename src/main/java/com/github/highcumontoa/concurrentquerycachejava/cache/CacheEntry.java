package com.github.highcumontoa.concurrentquerycachejava.cache;

/**
 * 不可变缓存条目。
 *
 * <p>时间线：createdAt ~ expireAt 之间为“新鲜”，正常命中；
 * expireAt ~ staleUntil 之间只能作为降级旧值（STALE_FALLBACK）返回；
 * 超过 staleUntil 后旧值也不再可用。
 */
public final class CacheEntry<V> {

    private final V value;
    private final long createdAtMillis;
    private final long expireAtMillis;
    private final long staleUntilMillis;

    CacheEntry(V value, long createdAtMillis, long ttlMillis, long staleAcceptedMillis) {
        this.value = value;
        this.createdAtMillis = createdAtMillis;
        this.expireAtMillis = createdAtMillis + ttlMillis;
        this.staleUntilMillis = this.expireAtMillis + staleAcceptedMillis;
    }

    /** 是否仍在有效期内，可作为正常结果返回。 */
    public boolean isFresh(long nowMillis) {
        return nowMillis < expireAtMillis;
    }

    /** 是否已过期但仍在降级宽限窗口内。 */
    public boolean isStaleUsable(long nowMillis) {
        return nowMillis >= expireAtMillis && nowMillis < staleUntilMillis;
    }

    public V getValue() {
        return value;
    }

    public long getCreatedAtMillis() {
        return createdAtMillis;
    }

    public long getExpireAtMillis() {
        return expireAtMillis;
    }

    public long getStaleUntilMillis() {
        return staleUntilMillis;
    }
}
