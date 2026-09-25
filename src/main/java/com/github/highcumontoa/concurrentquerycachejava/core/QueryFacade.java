package com.github.highcumontoa.concurrentquerycachejava.core;

import com.github.highcumontoa.concurrentquerycachejava.config.QueryCacheProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Optional;

/**
 * 查询编排：缓存 → 并发合并 → 并发上限 → 慢依赖计算 → 写缓存。
 * 计算失败或超时不写缓存；降级开关打开且有旧值时返回带降级标记的旧值。
 */
@Service
public class QueryFacade {

    private static final Logger log = LoggerFactory.getLogger(QueryFacade.class);

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
            RequestCoalescer.CoalescedResult<String> coalesced = coalescer.coalesce(key, () -> {
                // 双重检查：等待许可期间可能已有同键请求写入缓存
                Optional<String> recheck = cacheStore.getFresh(key);
                if (recheck.isPresent()) {
                    return recheck.get();
                }
                String value = limiter.execute(() -> dependency.compute(key));
                cacheStore.put(key, value);
                return value;
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

    private static long elapsedMs(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }
}
