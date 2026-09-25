package com.github.highcumontoa.concurrentquerycachejava.cache;

/**
 * 查询结果来源标记，用于区分正常结果与降级结果。
 */
public enum CacheStatus {
    /** 直接命中未过期缓存 */
    HIT,
    /** 本次请求触发（或合并到）一次底层计算 */
    COMPUTED,
    /** 底层失败但允许降级，返回了过期旧值 */
    STALE_FALLBACK,
    /** 查询失败（未取到可用结果） */
    FAILED
}
