package com.github.highcumontoa.concurrentquerycachejava.cache;

/**
 * 查询加速层对外抛出的失败异常，携带可区分的失败原因。
 */
public class CacheQueryException extends RuntimeException {

    private final transient FailureReason reason;

    public CacheQueryException(FailureReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public CacheQueryException(FailureReason reason, String message, Throwable cause) {
        super(message, cause);
        this.reason = reason;
    }

    public FailureReason getReason() {
        return reason;
    }
}
