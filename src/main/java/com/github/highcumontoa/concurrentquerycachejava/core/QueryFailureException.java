package com.github.highcumontoa.concurrentquerycachejava.core;

/**
 * 查询失败，携带可区分的失败原因。
 */
public class QueryFailureException extends RuntimeException {

    public enum Reason {
        /** 底层计算本身抛错。 */
        UPSTREAM_ERROR,
        /** 底层计算超时。 */
        COMPUTE_TIMEOUT,
        /** 排队容量已满，被背压拒绝。 */
        OVERLOADED,
        /** 排队等待许可超时。 */
        ACQUIRE_TIMEOUT,
        /** 同键合并等待者数量超过容量上限。 */
        COALESCER_OVERLOADED,
        /** 同键合并等待在途计算结果超时。 */
        COALESCE_WAIT_TIMEOUT,
        /** 在途计算与数据刷新反复交错，重算后仍非最新版本。 */
        REFRESH_CONFLICT
    }

    private final Reason reason;

    public QueryFailureException(Reason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public QueryFailureException(Reason reason, String message, Throwable cause) {
        super(message, cause);
        this.reason = reason;
    }

    public Reason getReason() {
        return reason;
    }
}
