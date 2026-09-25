package com.github.highcumontoa.concurrentquerycachejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 查询加速层配置项。
 */
@ConfigurationProperties(prefix = "query-cache")
public class QueryCacheProperties {

    /** 缓存条目有效期（毫秒）。 */
    private long ttlMillis = 1000;

    /** 底层暂时不可用时是否允许返回带降级标记的旧值。 */
    private boolean staleFallbackEnabled = true;

    /** 底层计算的最大并发数。 */
    private int maxConcurrency = 4;

    /** 等待执行许可的队列容量（超出即快速失败）。 */
    private int queueCapacity = 16;

    /** 排队等待许可的最大时长（毫秒），超时快速失败。 */
    private long acquireTimeoutMillis = 500;

    /** 单次底层计算的最大时长（毫秒），超时视为失败且不写缓存。 */
    private long computeTimeoutMillis = 2000;

    /** 模拟慢依赖的基础延迟（毫秒）。 */
    private long simulatedDelayMillis = 100;

    public long getTtlMillis() { return ttlMillis; }
    public void setTtlMillis(long ttlMillis) { this.ttlMillis = ttlMillis; }
    public boolean isStaleFallbackEnabled() { return staleFallbackEnabled; }
    public void setStaleFallbackEnabled(boolean staleFallbackEnabled) { this.staleFallbackEnabled = staleFallbackEnabled; }
    public int getMaxConcurrency() { return maxConcurrency; }
    public void setMaxConcurrency(int maxConcurrency) { this.maxConcurrency = maxConcurrency; }
    public int getQueueCapacity() { return queueCapacity; }
    public void setQueueCapacity(int queueCapacity) { this.queueCapacity = queueCapacity; }
    public long getAcquireTimeoutMillis() { return acquireTimeoutMillis; }
    public void setAcquireTimeoutMillis(long acquireTimeoutMillis) { this.acquireTimeoutMillis = acquireTimeoutMillis; }
    public long getComputeTimeoutMillis() { return computeTimeoutMillis; }
    public void setComputeTimeoutMillis(long computeTimeoutMillis) { this.computeTimeoutMillis = computeTimeoutMillis; }
    public long getSimulatedDelayMillis() { return simulatedDelayMillis; }
    public void setSimulatedDelayMillis(long simulatedDelayMillis) { this.simulatedDelayMillis = simulatedDelayMillis; }
}
