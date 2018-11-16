package com.alibaba.otter.canal.adapter.launcher.config;

import org.springframework.cloud.context.refresh.ContextRefresher;
import org.springframework.cloud.context.scope.refresh.RefreshScope;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RefresherConfig {

    @Bean
    public RefreshScope refreshScope() {
        return new RefreshScope();
    }

    @Bean
    public ContextRefresher contextRefresher(ConfigurableApplicationContext configurableApplicationContext,
                                             RefreshScope refreshScope) {
        return new ContextRefresher(configurableApplicationContext, refreshScope);
    }
}
