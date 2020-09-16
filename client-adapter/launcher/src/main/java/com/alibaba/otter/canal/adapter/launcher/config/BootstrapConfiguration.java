package com.alibaba.otter.canal.adapter.launcher.config;

import com.alibaba.otter.canal.adapter.launcher.monitor.remote.RemoteConfigLoader;
import com.alibaba.otter.canal.adapter.launcher.monitor.remote.RemoteConfigLoaderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

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

    // 通过此变量控制此类的loadRemoteConfig方法只被执行一次
    // 防止远程配置环境下执行contextRefresher.refresh()时调用到loadRemoteConfig再次触发文件更改事件，造成死循环
    private volatile boolean inited = false;

    @PostConstruct
    public void loadRemoteConfig() {
        if (!inited) {
            remoteConfigLoader = RemoteConfigLoaderFactory.getRemoteConfigLoader(env);
            if (remoteConfigLoader != null) {
                remoteConfigLoader.loadRemoteConfig();
                remoteConfigLoader.loadRemoteAdapterConfigs();
                remoteConfigLoader.startMonitor(); // 启动监听
                inited = true;
            }
        }
    }

    @PreDestroy
    public synchronized void destroy() {
        if (remoteConfigLoader != null) {
            remoteConfigLoader.destroy();
        }
    }
}
