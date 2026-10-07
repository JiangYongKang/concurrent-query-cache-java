package com.github.highcumontoa.concurrentquerycachejava.core;

import com.github.highcumontoa.concurrentquerycachejava.config.QueryCacheProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 可本地模拟的慢依赖：带可配置延迟、可注入故障，并统计实际计算次数。
 * 可设置阻塞闸门：计算开始后先报告"已启动"，再等待外部放行，
 * 用于确定性地模拟"计算在途期间发生数据刷新"的交错时序。
 */
@Component
public class SlowDependency {

    private static final Logger log = LoggerFactory.getLogger(SlowDependency.class);

    private final QueryCacheProperties properties;
    private final AtomicLong invocations = new AtomicLong();
    private final AtomicBoolean failing = new AtomicBoolean(false);
    private final AtomicReference<Long> delayOverride = new AtomicReference<>();
    private volatile CountDownLatch computeStarted;
    private volatile CountDownLatch allowComputeFinish;

    public SlowDependency(QueryCacheProperties properties) {
        this.properties = properties;
    }

    /** 执行一次慢计算。 */
    public String compute(String key) {
        long count = invocations.incrementAndGet();
        long delay = delayOverride.get() != null ? delayOverride.get() : properties.getSimulatedDelayMillis();
        log.info("[slow-dependency] compute start key={} invocation#={} delay={}ms", key, count, delay);
        CountDownLatch started = computeStarted;
        CountDownLatch gate = allowComputeFinish;
        if (started != null) {
            started.countDown();
        }
        if (gate != null) {
            try {
                log.info("[slow-dependency] compute key={} invocation#={} paused at gate", key, count);
                gate.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new QueryFailureException(QueryFailureException.Reason.UPSTREAM_ERROR,
                        "interrupted while blocked at gate for key=" + key, e);
            }
        }
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
        return "value-" + key + "-" + count;
    }

    /**
     * 安装阻塞闸门：每次计算开始后先 countDown {@code started}，
     * 然后阻塞等待 {@code allowFinish}，再继续。传 null 移除闸门。
     */
    public void setGate(CountDownLatch started, CountDownLatch allowFinish) {
        this.computeStarted = started;
        this.allowComputeFinish = allowFinish;
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
}
