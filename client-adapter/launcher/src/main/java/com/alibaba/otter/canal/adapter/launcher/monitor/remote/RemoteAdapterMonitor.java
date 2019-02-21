package com.alibaba.otter.canal.adapter.launcher.monitor.remote;

/**
 * 远程配置监听器接口
 *
 * @author rewerma 2019-01-25 下午05:20:16
 * @version 1.0.0
 */
public interface RemoteAdapterMonitor {

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
     * @param name 配置名
     */
    void onDelete(String name);
}
