package com.alibaba.otter.canal.prometheus;

import com.alibaba.otter.canal.spi.CanalMetricsProvider;
import com.alibaba.otter.canal.spi.CanalMetricsService;

/**
 * @author Chuanyi Li
 */
public class PrometheusProvider implements CanalMetricsProvider {

    @Override
    public CanalMetricsService getService() {
        return PrometheusService.getInstance();
    }
}
