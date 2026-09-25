package com.github.highcumontoa.concurrentquerycachejava.backend;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 可本地模拟的慢依赖（“底层数据源”）。
 *
 * <p>通过管理端/测试可控制：单次查询延迟 {@code delayMillis}、是否直接失败 {@code failing}、
 * 每个键的版本号 {@code version}（模拟底层状态变化），并统计实际调用次数 {@code invocationCount}，
 * 用于断言合并/缓存命中时底层确实只被调用一次。
 */
@Service
public class SlowBackendService {

    private static final Logger log = LoggerFactory.getLogger(SlowBackendService.class);

    private final AtomicInteger invocationCount = new AtomicInteger(0);
    private final ConcurrentHashMap<String, Long> versions = new ConcurrentHashMap<>();
    private volatile long delayMillis = 300;
    private volatile boolean failing = false;

    /**
     * 执行一次“慢查询”。若线程被中断（例如计算超时被取消），会抛出 InterruptedException，
     * 从而真正释放底层占用的计算资源。
     */
    public Map<String, Object> query(String key) throws InterruptedException {
        int seq = invocationCount.incrementAndGet();
        long start = System.currentTimeMillis();
        log.info("backend invoke start key={} seq={} delayMs={} failing={}", key, seq, delayMillis, failing);
        try {
            Thread.sleep(delayMillis);
        } catch (InterruptedException e) {
            log.warn("backend invoke interrupted key={} seq={} elapsedMs={}",
                    key, seq, System.currentTimeMillis() - start);
            throw e;
        }
        if (failing) {
            log.warn("backend invoke failure key={} seq={} elapsedMs={}",
                    key, seq, System.currentTimeMillis() - start);
            throw new IllegalStateException("simulated backend failure for key: " + key);
        }
        long version = versions.getOrDefault(key, 1L);
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("key", key);
        data.put("value", "v" + version);
        data.put("version", version);
        data.put("computedAt", start);
        log.info("backend invoke done key={} seq={} version={} elapsedMs={}",
                key, seq, version, System.currentTimeMillis() - start);
        return data;
    }

    public void setFailing(boolean failing) {
        this.failing = failing;
    }

    public boolean isFailing() {
        return failing;
    }

    public void setDelayMillis(long delayMillis) {
        this.delayMillis = delayMillis;
    }

    public long getDelayMillis() {
        return delayMillis;
    }

    public int getInvocationCount() {
        return invocationCount.get();
    }

    public void reset() {
        invocationCount.set(0);
        versions.clear();
        failing = false;
        delayMillis = 300;
    }

    public long currentVersion(String key) {
        return versions.getOrDefault(key, 1L);
    }

    /** 模拟底层状态变化：版本号 +1（初始版本为 1，首次 bump 后为 2）。 */
    public long bumpVersion(String key) {
        long v = versions.compute(key, (k, old) -> old == null ? 2L : old + 1L);
        log.info("backend version bumped key={} newVersion={}", key, v);
        return v;
    }
}
