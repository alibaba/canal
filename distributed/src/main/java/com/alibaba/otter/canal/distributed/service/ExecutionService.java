package com.alibaba.otter.canal.distributed.service;

/**
 * 分布式服务执行接口
 *
 * @author rewerma 2020-11-8 下午03:07:11
 * @version 1.0.0
 */
public interface ExecutionService {

    /**
     * 启动实例
     * 
     * @param instance 实例名称
     */
    void start(String instance);

    /**
     * 停止实例
     *
     * @param instance 实例名称
     */
    void stop(String instance);
}
