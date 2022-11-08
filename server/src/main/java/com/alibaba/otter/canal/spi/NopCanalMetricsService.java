package com.alibaba.otter.canal.spi;

import com.alibaba.otter.canal.instance.core.CanalInstance;

/**
 * @author Chuanyi Li
 */
public class NopCanalMetricsService implements CanalMetricsService {

    public static final NopCanalMetricsService NOP = new NopCanalMetricsService();

    private NopCanalMetricsService() {}

    @Override
    public void initialize() {

    }

    @Override
    public void terminate() {

    }

    @Override
    public boolean isRunning() {
        return false;
    }

    @Override
    public void register(CanalInstance instance) {

    }

    @Override
    public void unregister(CanalInstance instance) {

    }

    @Override
    public void setServerPort(int port) {

    }
}
