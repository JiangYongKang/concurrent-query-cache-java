package com.github.highcumontoa.concurrentquerycachejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 查询加速层配置项（前缀 query-cache）。
 *
 * <ul>
 *   <li>ttl-millis：缓存条目有效期；过期后不再作为正常结果返回。</li>
 *   <li>stale-accepted-millis：过期后旧值仍可用于降级的最长宽限时间。</li>
 *   <li>stale-on-failure-allowed：底层失败/超时时是否允许返回带降级标记的旧值（总开关）。</li>
 *   <li>compute-concurrency：底层计算全局并发上限（许可数）。</li>
 *   <li>max-waiters-per-key：单个键允许合并的等待者上限；超出立即以 QUEUE_FULL 快速失败。</li>
 *   <li>wait-timeout-millis：合并等待者最长等待时间；超时以 WAIT_TIMEOUT 失败。</li>
 *   <li>compute-timeout-millis：单次底层计算最长耗时；超时以 COMPUTE_TIMEOUT 失败且不写缓存。</li>
 * </ul>
 */
@ConfigurationProperties(prefix = "query-cache")
public class QueryCacheProperties {

    /** 缓存有效期（毫秒）。 */
    private long ttlMillis = 5000;

    /** 过期旧值可用于降级的宽限窗口（毫秒）。 */
    private long staleAcceptedMillis = 15000;

    /** 底层暂时不可用时是否允许降级返回旧值。 */
    private boolean staleOnFailureAllowed = false;

    /** 底层计算并发上限。 */
    private int computeConcurrency = 2;

    /** 单键合并等待队列容量（不含正在计算的 leader）。 */
    private int maxWaitersPerKey = 10;

    /** 合并等待超时（毫秒）。 */
    private long waitTimeoutMillis = 1000;

    /** 底层计算超时（毫秒）。 */
    private long computeTimeoutMillis = 2000;

    public long getTtlMillis() {
        return ttlMillis;
    }

    public void setTtlMillis(long ttlMillis) {
        this.ttlMillis = ttlMillis;
    }

    public long getStaleAcceptedMillis() {
        return staleAcceptedMillis;
    }

    public void setStaleAcceptedMillis(long staleAcceptedMillis) {
        this.staleAcceptedMillis = staleAcceptedMillis;
    }

    public boolean isStaleOnFailureAllowed() {
        return staleOnFailureAllowed;
    }

    public void setStaleOnFailureAllowed(boolean staleOnFailureAllowed) {
        this.staleOnFailureAllowed = staleOnFailureAllowed;
    }

    public int getComputeConcurrency() {
        return computeConcurrency;
    }

    public void setComputeConcurrency(int computeConcurrency) {
        this.computeConcurrency = computeConcurrency;
    }

    public int getMaxWaitersPerKey() {
        return maxWaitersPerKey;
    }

    public void setMaxWaitersPerKey(int maxWaitersPerKey) {
        this.maxWaitersPerKey = maxWaitersPerKey;
    }

    public long getWaitTimeoutMillis() {
        return waitTimeoutMillis;
    }

    public void setWaitTimeoutMillis(long waitTimeoutMillis) {
        this.waitTimeoutMillis = waitTimeoutMillis;
    }

    public long getComputeTimeoutMillis() {
        return computeTimeoutMillis;
    }

    public void setComputeTimeoutMillis(long computeTimeoutMillis) {
        this.computeTimeoutMillis = computeTimeoutMillis;
    }
}
