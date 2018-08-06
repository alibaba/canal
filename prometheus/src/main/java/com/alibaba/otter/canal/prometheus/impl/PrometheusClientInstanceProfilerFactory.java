package com.alibaba.otter.canal.prometheus.impl;

import com.alibaba.otter.canal.server.netty.ClientInstanceProfiler;
import com.alibaba.otter.canal.server.netty.ClientInstanceProfilerFactory;

/**
 * @author Chuanyi Li
 */
public class PrometheusClientInstanceProfilerFactory implements ClientInstanceProfilerFactory {

    @Override
    public ClientInstanceProfiler create(String destination) {
        return new PrometheusClientInstanceProfiler(destination);
    }
}
