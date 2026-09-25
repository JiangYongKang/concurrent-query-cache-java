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
        ACQUIRE_TIMEOUT
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
