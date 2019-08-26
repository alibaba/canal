package com.alibaba.otter.canal.admin.connector;

import com.alibaba.otter.canal.admin.common.exception.ServiceException;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;

/**
 * canal数据操作客户端
 * 
 * @author zebin.xuzb @ 2012-6-19
 * @author jianghang
 * @version 1.0.0
 */
public interface AdminConnector {

    /**
     * 链接对应的canal server
     * 
     * @throws CanalClientException
     */
    void connect() throws ServiceException;

    /**
     * 释放链接
     * 
     * @throws CanalClientException
     */
    void disconnect() throws ServiceException;

    /**
     * 获取Canal Server状态
     *
     * @return 状态代码
     */
    boolean check();

    /**
     * 启动Canal Server
     *
     * @return 是否成功
     */
    boolean start();

    /**
     * 停止Canal Server
     *
     * @return 是否成功
     */
    boolean stop();

    /**
     * 重启Canal Server
     *
     * @return 是否成功
     */
    boolean restart();

    /**
     * 获取所有当前节点下运行中的实例
     *
     * @return 实例信息
     */
    String getRunningInstances();

    /**
     * 通过实例名检查
     * 
     * @param destination
     * @return
     */
    boolean checkInstance(String destination);

    /**
     * 通过实例名启动实例
     *
     * @param destination 实例名
     * @return 是否成功
     */
    boolean startInstance(String destination);

    /**
     * 通过实例名关闭实例
     *
     * @param destination 实例名
     * @return 是否成功
     */
    boolean stopInstance(String destination);

    /**
     * 通过实例名重启实例
     *
     * @param destination 实例名
     * @return 是否成功
     */
    boolean restartInstance(String destination);

    /**
     * 获取Canal Server日志列表
     *
     * @return 日志信息
     */
    String listCanalLog();

    /**
     * 获取Canal Server日志
     *
     * @return 日志信息
     */
    String canalLog(int lines);

    /**
     * 获取Instance的机器日志列表
     * 
     * @param destination
     */
    String listInstanceLog(String destination);

    /**
     * 通过实例名获取实例日志
     *
     * @return 日志信息
     */
    String instanceLog(String destination, String fileName, int lines);

}
