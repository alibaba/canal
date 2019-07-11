package com.alibaba.otter.canal.deployer.mbean;

import com.alibaba.otter.canal.deployer.CanalLauncher;
import com.alibaba.otter.canal.deployer.CanalStater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public void setStatus(int status) {
        this.status = status;
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

}
