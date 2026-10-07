package com.github.highcumontoa.concurrentquerycachejava.core;

import com.github.highcumontoa.concurrentquerycachejava.config.QueryCacheProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Optional;

/**
 * 查询编排：缓存 → 并发合并 → 并发上限 → 慢依赖计算 → 带世代校验地写缓存。
 * 计算失败或超时不写缓存；降级开关打开且有同世代旧值时返回带降级标记的旧值。
 *
 * <p>数据刷新（{@link #refresh} / {@link #refreshAll}）会推进被波及键的世代：
 * 世代推进前启动、之后才结束的计算结果既不会对外返回，也不会写入缓存，
 * 发起方会在新世代下重新计算，因此刷新后的第一次查询必然反映新状态。
 */
@Service
public class QueryFacade {

    private static final Logger log = LoggerFactory.getLogger(QueryFacade.class);

    /** 世代被刷新作废后的最大重算次数，防止刷新风暴下无限重试。 */
    private static final int MAX_STALE_ATTEMPTS = 3;

    private final QueryCacheProperties properties;
    private final QueryCacheStore cacheStore;
    private final RequestCoalescer coalescer;
    private final ConcurrencyLimiter limiter;
    private final SlowDependency dependency;

    public QueryFacade(QueryCacheProperties properties,
                       QueryCacheStore cacheStore,
                       RequestCoalescer coalescer,
                       ConcurrencyLimiter limiter,
                       SlowDependency dependency) {
        this.properties = properties;
        this.cacheStore = cacheStore;
        this.coalescer = coalescer;
        this.limiter = limiter;
        this.dependency = dependency;
    }

    /** 查询入口。 */
    public QueryResult query(String key) {
        long start = System.nanoTime();

        Optional<String> fresh = cacheStore.getFresh(key);
        if (fresh.isPresent()) {
            QueryResult result = new QueryResult(key, fresh.get(), QuerySource.CACHE, elapsedMs(start));
            log.info("[query] key={} source={} elapsed={}ms", key, result.source(), result.elapsedMillis());
            return result;
        }

        try {
            RequestCoalescer.CoalescedResult<String> coalesced = coalescer.coalesce(
                    key,
                    properties.getCoalesceWaitCapacity(),
                    properties.getCoalesceWaitTimeoutMillis(),
                    () -> computeFresh(key));
            QuerySource source = coalesced.coalesced() ? QuerySource.COALESCED : QuerySource.COMPUTED;
            QueryResult result = new QueryResult(key, coalesced.value(), source, elapsedMs(start));
            log.info("[query] key={} source={} elapsed={}ms", key, result.source(), result.elapsedMillis());
            return result;
        } catch (QueryFailureException e) {
            if (properties.isStaleFallbackEnabled()) {
                Optional<String> stale = cacheStore.getStale(key);
                if (stale.isPresent()) {
                    QueryResult result = new QueryResult(key, stale.get(), QuerySource.STALE_DEGRADED, elapsedMs(start));
                    log.warn("[query] key={} source={} reason={} elapsed={}ms",
                            key, result.source(), e.getReason(), result.elapsedMillis());
                    return result;
                }
            }
            log.warn("[query] key={} failed reason={} elapsed={}ms", key, e.getReason(), elapsedMs(start));
            throw e;
        }
    }

    /**
     * 发起方计算：双重检查缓存 → 捕获世代 → 限流计算 → 世代校验后提交。
     * 期间若发生数据刷新（世代推进），作废本次结果并在新世代下重算。
     */
    private String computeFresh(String key) {
        for (int attempt = 1; ; attempt++) {
            Optional<String> recheck = cacheStore.getFresh(key);
            if (recheck.isPresent()) {
                return recheck.get();
            }
            long generation = cacheStore.currentGeneration(key);
            String value = limiter.execute(() -> dependency.compute(key));
            if (cacheStore.commitIfCurrent(key, generation, value)) {
                return value;
            }
            log.warn("[query] key={} stale compute rejected by data refresh, attempt={}", key, attempt);
            if (attempt >= MAX_STALE_ATTEMPTS) {
                throw new QueryFailureException(QueryFailureException.Reason.UPSTREAM_ERROR,
                        "compute result for key=" + key + " was repeatedly invalidated by data refresh");
            }
        }
    }

    /** 显式失效某个键（不推进世代，原有语义不变）。 */
    public void invalidate(String key) {
        cacheStore.invalidate(key);
        log.info("[query] key={} invalidated", key);
    }

    /** 后台数据刷新驱动失效：单键立即生效，在途旧结果作废。 */
    public void refresh(String key) {
        cacheStore.refresh(key);
        log.info("[query] key={} data refreshed; generation advanced, cache evicted", key);
    }

    /** 后台批量刷新同一批相关键；未被波及的键继续正常命中缓存。 */
    public void refreshAll(Collection<String> keys) {
        cacheStore.refreshAll(keys);
        log.info("[query] keys={} batch data refreshed; {} keys advanced", keys, keys.size());
    }

    /** 按前缀批量刷新（如某数据集下的所有分片），返回被波及的键数量。 */
    public int refreshByPrefix(String prefix) {
        int affected = cacheStore.refreshByPrefix(prefix);
        log.info("[query] prefix={} batch data refreshed; {} keys advanced", prefix, affected);
        return affected;
    }

    private static long elapsedMs(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }
}
