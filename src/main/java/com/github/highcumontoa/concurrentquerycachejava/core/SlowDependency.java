package com.github.highcumontoa.concurrentquerycachejava.core;

import com.github.highcumontoa.concurrentquerycachejava.config.QueryCacheProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 可本地模拟的慢依赖：带可配置延迟、可注入故障，并统计实际计算次数。
 * 另维护一份“后台数据”，可通过 refresh 单键或批量推送新状态，
 * 推送之后触发的计算必须返回新状态。
 */
@Component
public class SlowDependency {

    private static final Logger log = LoggerFactory.getLogger(SlowDependency.class);

    private final QueryCacheProperties properties;
    private final AtomicLong invocations = new AtomicLong();
    private final AtomicBoolean failing = new AtomicBoolean(false);
    private final AtomicReference<Long> delayOverride = new AtomicReference<>();
    private final Map<String, String> backendData = new ConcurrentHashMap<>();
    private volatile Runnable onComputeStarted = () -> { };

    public SlowDependency(QueryCacheProperties properties) {
        this.properties = properties;
    }

    /**
     * 执行一次慢计算。在计算启动时快照该键当前的后台数据，随后模拟慢处理；
     * 因此“启动前读取、处理期间后台刷新”的在途计算拿到的仍是旧快照。
     */
    public String compute(String key) {
        long count = invocations.incrementAndGet();
        String snapshot = backendData.get(key);
        onComputeStarted.run();
        long delay = delayOverride.get() != null ? delayOverride.get() : properties.getSimulatedDelayMillis();
        log.info("[slow-dependency] compute start key={} invocation#={} delay={}ms snapshot={}", key, count, delay, snapshot);
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QueryFailureException(QueryFailureException.Reason.UPSTREAM_ERROR,
                    "interrupted while computing key=" + key, e);
        }
        if (failing.get()) {
            throw new QueryFailureException(QueryFailureException.Reason.UPSTREAM_ERROR,
                    "simulated dependency failure for key=" + key);
        }
        return snapshot != null ? snapshot : "value-" + key + "-" + count;
    }

    /** 实际底层计算被触发的次数。 */
    public long invocationCount() {
        return invocations.get();
    }

    /** 让后续计算失败，用于本地模拟依赖故障。 */
    public void setFailing(boolean failing) {
        this.failing.set(failing);
    }

    /** 覆盖计算延迟（毫秒），传 null 恢复配置默认值。 */
    public void setDelayOverride(Long delayMillis) {
        this.delayOverride.set(delayMillis);
    }

    /** 注册计算开始回调（在延迟之前触发），用于测试在途计算与刷新的交错。传 null 清除。 */
    public void setOnComputeStarted(Runnable callback) {
        this.onComputeStarted = callback != null ? callback : () -> { };
    }

    /** 后台刷新单个键的数据：推送新状态，推送之后触发的计算将返回该值。 */
    public void refresh(String key, String newValue) {
        backendData.put(key, newValue);
        log.info("[slow-dependency] backend refresh key={} newValue={}", key, newValue);
    }

    /** 后台批量刷新一组相关键的数据。 */
    public void refreshAll(Map<String, String> updates) {
        updates.forEach((key, value) -> backendData.put(key, value));
        log.info("[slow-dependency] backend batch refresh keys={}", updates.keySet());
    }
}
