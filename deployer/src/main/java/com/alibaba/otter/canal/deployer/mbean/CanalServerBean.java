package com.alibaba.otter.canal.deployer.mbean;

import com.alibaba.otter.canal.deployer.CanalLauncher;
import com.alibaba.otter.canal.deployer.CanalStater;
import com.alibaba.otter.canal.deployer.InstanceConfig;
import com.alibaba.otter.canal.deployer.monitor.InstanceAction;
import com.alibaba.otter.canal.deployer.monitor.InstanceConfigMonitor;
import com.alibaba.otter.canal.deployer.monitor.SpringInstanceConfigMonitor;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CanalServerBean implements CanalServerMXBean {

    private static final Logger logger = LoggerFactory.getLogger(CanalServerBean.class);

    private volatile int        status;

    private CanalStater         canalStater;

    public CanalServerBean(CanalStater canalStater){
        this.canalStater = canalStater;
        this.status = canalStater.isRunning() ? 1 : 0;
    }

    @Override
    public int getStatus() {
        return status;
    }

    @Override
    public synchronized boolean start() {
        try {
            if (!canalStater.isRunning()) {
                canalStater.start();
                status = 1;
                return true;
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
        return false;
    }

    @Override
    public synchronized boolean stop() {
        try {
            if (canalStater.isRunning()) {
                canalStater.stop();
                status = 0;
                return true;
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
        return false;
    }

    @Override
    public synchronized boolean restart() {
        stop();
        return start();
    }

    @Override
    public synchronized boolean exit() {
        stop();
        CanalLauncher.runningLatch.countDown();
        return true;
    }

    @Override
    public synchronized boolean startInstance(String destination) {
        try {
            InstanceAction instanceAction = getInstanceAction(destination);
            if (instanceAction != null) {
                instanceAction.start(destination);
                return true;
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
        return false;
    }

    @Override
    public synchronized boolean stopInstance(String destination) {
        try {
            InstanceAction instanceAction = getInstanceAction(destination);
            if (instanceAction != null) {
                instanceAction.stop(destination);
                return true;
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
        return false;
    }

    @Override
    public synchronized boolean reloadInstance(String destination) {
        try {
            InstanceAction instanceAction = getInstanceAction(destination);
            if (instanceAction != null) {
                instanceAction.reload(destination);
                return true;
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
        return false;
    }

    @Override
    public synchronized String getRunningInstances() {
        try {
            Map<String, InstanceConfig> instanceConfigs = canalStater.getController().getInstanceConfigs();
            if (instanceConfigs != null) {
                return Joiner.on(",").join(instanceConfigs.keySet());
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
        return "";
    }

    private InstanceAction getInstanceAction(String destination) {
        Map<InstanceConfig.InstanceMode, InstanceConfigMonitor> monitors = canalStater.getController()
            .getInstanceConfigMonitors();
        SpringInstanceConfigMonitor monitor = (SpringInstanceConfigMonitor) monitors
            .get(InstanceConfig.InstanceMode.SPRING);
        Map<String, InstanceAction> instanceActions = monitor.getActions();
        return instanceActions.get(destination);
    }

}
