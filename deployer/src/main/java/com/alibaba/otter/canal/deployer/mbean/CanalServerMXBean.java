package com.alibaba.otter.canal.deployer.mbean;

public interface CanalServerMXBean {

    int getStatus();

    boolean start();

    boolean stop();

    boolean restart();

    boolean exit();

    boolean startInstance(String destination);

    boolean stopInstance(String destination);

    boolean reloadInstance(String destination);

    String getRunningInstances();
}