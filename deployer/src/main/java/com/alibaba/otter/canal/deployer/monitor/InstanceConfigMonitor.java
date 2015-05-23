package com.alibaba.otter.canal.deployer.monitor;

import com.alibaba.otter.canal.common.CanalLifeCycle;

/**
 * 监听instance file的文件变化，触发instance start/stop等操作
 * 
 * @author jianghang 2013-2-6 下午06:19:56
 * @version 1.0.1
 */
public interface InstanceConfigMonitor extends CanalLifeCycle {

    void register(String destination, InstanceAction action);

    void unregister(String destination);
}
