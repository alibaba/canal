package com.alibaba.otter.canal.prometheus;

import com.alibaba.otter.canal.prometheus.impl.InboundThroughputAspect;
import com.alibaba.otter.canal.prometheus.impl.OutboundThroughputAspect;

/**
 * @author Chuanyi Li
 */
public class CanalServerExports {

    private static boolean initialized = false;

    public static synchronized void initialize() {
        if (!initialized) {
            InboundThroughputAspect.getCollector().register();
            OutboundThroughputAspect.getCollector().register();
            initialized = true;
        }
    }

}
