package com.github.highcumontoa.concurrentquerycachejava.core;

import com.github.highcumontoa.concurrentquerycachejava.config.QueryCacheProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 查询编排：缓存 → 并发合并 → 并发上限 → 慢依赖计算 → 版本校验写缓存。
 * 计算失败或超时不写缓存；降级开关打开且有同版本旧值时返回带降级标记的旧值。
 *
 * 数据刷新（refresh）：提升受影响键的数据版本并移除缓存条目。
 * 变更前启动、变更后结束的在途计算在提交时因版本不符被丢弃（绝不返回、绝不入缓存），
 * 发起方会基于新版本重算一次，使合并等待方与变更后的首个查询都拿到新状态。
 */
@Service
public class QueryFacade {

    private static final Logger log = LoggerFactory.getLogger(QueryFacade.class);

    /** 与数据刷新交错时，发起方最多重算的次数（含首次）。 */
    private static final int MAX_ATTEMPTS = 3;

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

        Optional<QueryCacheStore.Snapshot> fresh = cacheStore.getFresh(key);
        if (fresh.isPresent()) {
            QueryResult result = new QueryResult(key, fresh.get().value(), QuerySource.CACHE, elapsedMs(start));
            log.info("[query] key={} source={} elapsed={}ms", key, result.source(), result.elapsedMillis());
            return result;
        }

        try {
            RequestCoalescer.CoalescedResult<String> coalesced = coalescer.coalesce(key, () -> {
                for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
                    // 双重检查：等待许可期间可能已有同键请求写入缓存
                    Optional<QueryCacheStore.Snapshot> recheck = cacheStore.getFresh(key);
                    if (recheck.isPresent()) {
                        return recheck.get().value();
                    }
                    long expectedVersion = cacheStore.currentVersion(key);
                    String value = limiter.execute(() -> dependency.compute(key));
                    if (cacheStore.putIfVersionMatches(key, value, expectedVersion)) {
                        return value;
                    }
                    // 期间发生数据刷新：本次结果作废，绝不返回/入缓存，基于新版本重算
                    log.warn("[query] key={} stale computation discarded (attempt={}, expectedVersion={})",
                            key, attempt, expectedVersion);
                }
                throw new QueryFailureException(QueryFailureException.Reason.REFRESH_CONFLICT,
                        "data kept refreshing during computation for key=" + key
                                + ", result discarded after " + MAX_ATTEMPTS + " attempts");
            });
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

    /** 显式失效某个键。 */
    public void invalidate(String key) {
        cacheStore.invalidate(key);
        log.info("[query] key={} invalidated", key);
    }

    /** 后台刷新单个键：推送新数据并使该键缓存（含旧值与在途结果）立即作废。 */
    public void refresh(String key, String newValue) {
        dependency.refresh(key, newValue);
        cacheStore.refresh(List.of(key));
        log.info("[query] key={} refreshed -> cache invalidated", key);
    }

    /** 后台批量刷新一组相关键：仅作批量内的键，其余键继续命中缓存。 */
    public void refreshBatch(Map<String, String> updates) {
        dependency.refreshAll(updates);
        cacheStore.refresh(updates.keySet());
        log.info("[query] keys={} batch refreshed -> cache invalidated", updates.keySet());
    }

    private static long elapsedMs(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }
}
