package com.alibaba.otter.canal.adapter.launcher.config;

import javax.annotation.PostConstruct;

import com.alibaba.otter.canal.adapter.launcher.monitor.AdapterRemoteConfigMonitor;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

public class BootstrapConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(BootstrapConfiguration.class);

    @Autowired
    private Environment         env;

    @PostConstruct
    public void init() {
        try {
            // 加载远程配置
            String jdbcUrl = env.getProperty("canal.manager.jdbc.url");
            if (StringUtils.isNotEmpty(jdbcUrl)) {
                String jdbcUsername = env.getProperty("canal.manager.jdbc.username");
                String jdbcPassword = env.getProperty("canal.manager.jdbc.password");
                AdapterRemoteConfigMonitor configMonitor = new AdapterRemoteConfigMonitor(jdbcUrl,
                    jdbcUsername,
                    jdbcPassword);
                configMonitor.loadRemoteConfig();
                configMonitor.loadRemoteAdapterConfigs();
                configMonitor.start(); // 启动监听
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}
