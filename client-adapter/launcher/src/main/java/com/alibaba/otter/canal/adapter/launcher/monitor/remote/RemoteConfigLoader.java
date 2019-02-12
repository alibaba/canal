package com.alibaba.otter.canal.adapter.launcher.monitor.remote;

/**
 * 远程配置装载器接口
 *
 * @author rewerma 2019-01-25 下午05:20:16
 * @version 1.0.0
 */
public interface RemoteConfigLoader {

    /**
     * 加载远程application.yml配置到本地
     */
    void loadRemoteConfig();

    /**
     * 加载adapter配置
     */
    void loadRemoteAdapterConfigs();

    /**
     * 启动监听数据库变化
     */
    void startMonitor();

    /**
     * 销毁
     */
    void destroy();
}
