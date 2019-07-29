package com.alibaba.otter.canal.admin.jmx;

/**
 * Canal Server JMX MBean
 *
 * @author rewerma 2019-07-13 下午05:12:16
 * @version 1.0.0
 */
public interface CanalServerMXBean {

    /**
     * 获取Canal Server状态
     *
     * @return 状态代码
     */
    int getStatus();

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
     * 退出Canal Server(关闭进程)
     *
     * @return 是否成功
     */
    boolean exit();

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
    boolean reloadInstance(String destination);

    /**
     * 获取所有当前节点下运行中的实例
     *
     * @return 实例信息
     */
    String getRunningInstances();

    /**
     * 获取Canal Server日志(末尾100行)
     *
     * @return 日志信息
     */
    String canalLog();

    /**
     * 通过实例名获取实例日志(末尾100行)
     *
     * @return 日志信息
     */
    String instanceLog(String destination);
}
