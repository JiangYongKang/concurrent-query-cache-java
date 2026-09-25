package com.github.highcumontoa.concurrentquerycachejava.config;

import com.github.highcumontoa.concurrentquerycachejava.cache.QueryCache;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

/**
 * 查询加速层的 Spring 装配。
 */
@Configuration
@EnableConfigurationProperties(QueryCacheProperties.class)
public class QueryCacheConfig {

    @Bean(destroyMethod = "shutdown")
    public QueryCache<Map<String, Object>> queryCache(QueryCacheProperties properties) {
        return new QueryCache<>(properties);
    }
}
