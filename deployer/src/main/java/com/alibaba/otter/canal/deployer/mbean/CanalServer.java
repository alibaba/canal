package com.alibaba.otter.canal.deployer.mbean;

import com.alibaba.otter.canal.deployer.CanalLauncher;
import com.alibaba.otter.canal.deployer.CanalStater;

public class CanalServer implements CanalServerMXBean {

    private volatile int status;

    private CanalStater  canalStater;

    public CanalServer(CanalStater canalStater){
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
            e.printStackTrace();
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
            e.printStackTrace();
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
        CanalLauncher.running = false;
        return true;
    }

}
