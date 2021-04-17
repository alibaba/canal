package com.alibaba.otter.canal.distributed.service;

/**
 * 分布式服务接口
 *
 * @author rewerma 2020-11-8 下午03:07:11
 * @version 1.0.0
 */
public interface DistributedService {

    /**
     * 注册一个节点
     * 
     * @param nodeName 节点名称
     */
    void registerNode(String nodeName);

    /**
     * 释放一个节点
     */
    void releaseNode();

    /**
     * 分配实例节点
     */
    void distribute();

    /**
     * 实例是否属于当前节点
     * 
     * @param instance 实例名
     * @return true/false
     */
    boolean containInstance(String instance);

    /**
     * 新增一个实例
     * 
     * @param instance 实例名
     * @return 是否添加到当前节点
     */
    boolean addInstance(String instance);

    /**
     * 删除一个实例
     * 
     * @param instance 实例名
     */
    void removeInstance(String instance);

    /**
     * 从当前节点中移除运行中的实例, 用于实例迁移时
     *
     * @param instance 实例名
     */
    boolean removeInstanceFromCurrentNode(String instance);
}
