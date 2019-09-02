package com.alibaba.otter.canal.deployer.monitor;

/**
 * config配置变化
 * 
 * @author jianghang 2013-2-18 下午01:19:29
 * @version 1.0.1
 */
public interface InstanceAction {

    /**
     * 启动destination
     */
    void start(String destination);

    /**
     * 主动释放destination运行
     */
    void release(String destination);

    /**
     * 停止destination
     */
    void stop(String destination);

    /**
     * 重载destination，可能需要stop,start操作，或者只是更新下内存配置
     */
    void reload(String destination);
}
