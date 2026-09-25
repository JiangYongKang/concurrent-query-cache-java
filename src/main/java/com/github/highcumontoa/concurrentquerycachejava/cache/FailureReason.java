package com.github.highcumontoa.concurrentquerycachejava.cache;

/**
 * 查询失败的可区分原因。
 */
public enum FailureReason {
    /** 排队积压超过容量（背压，快速失败） */
    QUEUE_FULL,
    /** 等待合并结果的时间超过等待超时 */
    WAIT_TIMEOUT,
    /** 底层计算耗时超过计算超时 */
    COMPUTE_TIMEOUT,
    /** 底层计算显式失败（抛异常或返回错误） */
    COMPUTE_ERROR,
    /** 请求被中断/取消 */
    CANCELLED
}
