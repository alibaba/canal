package com.alibaba.otter.canal.deployer.monitor.remote;

import java.util.Properties;

/**
 * 远程配置装载器接口
 *
 * @author rewerma 2019-01-25 下午05:20:16
 * @version 1.0.0
 */
public interface RemoteConfigLoader {

    /**
     * 加载远程 canal.properties文件
     *
     * @return 远程配置的properties
     */
    Properties loadRemoteConfig();

    /**
     * 加载远程的instance配置
     */
    void loadRemoteInstanceConfigs();

    /**
     * 启动监听 canal 主配置和 instance 配置变化
     *
     * @param remoteCanalConfigMonitor 监听回调方法
     */
    void startMonitor(final RemoteCanalConfigMonitor remoteCanalConfigMonitor);

    /**
     * 销毁
     */
    void destroy();
}
