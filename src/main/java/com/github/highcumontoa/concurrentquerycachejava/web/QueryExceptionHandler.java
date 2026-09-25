package com.github.highcumontoa.concurrentquerycachejava.web;

import com.github.highcumontoa.concurrentquerycachejava.core.QueryFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/**
 * 将可区分的失败原因映射为不同的 HTTP 状态码。
 */
@RestControllerAdvice
public class QueryExceptionHandler {

    @ExceptionHandler(QueryFailureException.class)
    public ResponseEntity<Map<String, String>> handle(QueryFailureException ex) {
        HttpStatus status = switch (ex.getReason()) {
            case OVERLOADED, ACQUIRE_TIMEOUT -> HttpStatus.TOO_MANY_REQUESTS;
            case COMPUTE_TIMEOUT -> HttpStatus.GATEWAY_TIMEOUT;
            case UPSTREAM_ERROR -> HttpStatus.BAD_GATEWAY;
        };
        return ResponseEntity.status(status)
                .body(Map.of("reason", ex.getReason().name(), "message", ex.getMessage()));
    }
}
