package com.github.highcumontoa.concurrentquerycachejava.web;

import java.util.Map;

/**
 * 后台数据刷新请求体。
 * 单键刷新时使用 {@code key + value}；批量刷新时使用 {@code updates}（键 -> 新值）。
 */
public record RefreshRequest(String key, String value, Map<String, String> updates) {
}
