package com.alibaba.otter.canal.adapter.launcher.config;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

import com.alibaba.otter.canal.adapter.launcher.monitor.remote.RemoteConfigLoader;
import com.alibaba.otter.canal.adapter.launcher.monitor.remote.RemoteConfigLoaderFactory;

/**
 * Bootstrap级别配置加载
 *
 * @author rewerma @ 2019-01-05
 * @version 1.0.0
 */
public class BootstrapConfiguration {

    @Autowired
    private Environment        env;

    private RemoteConfigLoader remoteConfigLoader = null;

    @PostConstruct
    public void loadRemoteConfig() {
        remoteConfigLoader = RemoteConfigLoaderFactory.getRemoteConfigLoader(env);
        if (remoteConfigLoader != null) {
            remoteConfigLoader.loadRemoteConfig();
            remoteConfigLoader.loadRemoteAdapterConfigs();
            remoteConfigLoader.startMonitor(); // 启动监听
        }
    }

    @PreDestroy
    public synchronized void destroy() {
        if (remoteConfigLoader != null) {
            remoteConfigLoader.destroy();
        }
    }
}
