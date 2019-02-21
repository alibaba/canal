package com.alibaba.otter.canal.deployer.monitor.remote;

/**
 * 远程xxx/instance.properties配置监听器接口
 *
 * @author rewerma 2019-01-25 下午05:20:16
 * @version 1.0.0
 */
public interface RemoteInstanceMonitor {

    /**
     * 新增配置事件
     *
     * @param configItem 配置项
     */
    void onAdd(ConfigItem configItem);

    /**
     * 修改配置事件
     *
     * @param configItem 配置项
     */
    void onModify(ConfigItem configItem);

    /**
     * 删除配置事件
     *
     * @param instanceName 实例名
     */
    void onDelete(String instanceName);
}
