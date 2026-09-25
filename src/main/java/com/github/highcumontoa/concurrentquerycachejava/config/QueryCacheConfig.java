package com.github.highcumontoa.concurrentquerycachejava.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(QueryCacheProperties.class)
public class QueryCacheConfig {
}
