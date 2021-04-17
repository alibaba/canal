package com.alibaba.otter.canal.distributed.service;

import java.util.List;

/**
 * 监听器服务接口
 *
 * @author rewerma 2020-11-8 下午03:07:11
 * @version 1.0.0
 */
public interface MonitorService {

    /**
     * 初始化集群节点
     *
     * @param nodeName 节点名称
     */
    void init(String nodeName);

    /**
     * 监听节点变化
     *
     * @param listener 监听器回调函数
     * @return 初试返回所有节点
     */
    List<String> listen(Listener listener);

    /**
     * 销毁监听
     */
    void destroy();

    interface Listener {

        /**
         * 监听器回调函数
         * 
         * @param nodes 变化后的节点列表
         */
        void listen(List<String> nodes);
    }
}
