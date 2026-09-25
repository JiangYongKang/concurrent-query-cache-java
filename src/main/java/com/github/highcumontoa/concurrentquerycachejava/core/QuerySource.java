package com.github.highcumontoa.concurrentquerycachejava.core;

/**
 * 查询结果来源，用于区分正常结果与降级结果。
 */
public enum QuerySource {
    /** 本次请求触发了底层计算。 */
    COMPUTED,
    /** 并发合并：复用了同键正在进行的计算结果。 */
    COALESCED,
    /** 命中未过期缓存。 */
    CACHE,
    /** 底层不可用，返回带降级标记的旧值。 */
    STALE_DEGRADED
}
