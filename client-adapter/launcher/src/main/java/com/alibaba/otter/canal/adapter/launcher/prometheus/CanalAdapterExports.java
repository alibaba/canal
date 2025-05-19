package com.alibaba.otter.canal.adapter.launcher.prometheus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author sunxien
 * @date 2025/5/19
 * @since 1.0.0-SNAPSHOT
 */
public class CanalAdapterExports {

    private static final Logger logger = LoggerFactory.getLogger(CanalAdapterExports.class);

    private CanalAdapterExports() {
    }

    private static class SingletonHolder {
        private static final CanalAdapterExports SINGLETON = new CanalAdapterExports();
    }

    public static CanalAdapterExports instance() {
        return SingletonHolder.SINGLETON;
    }

    public void initialize() {
        // TODO
    }

    public void terminate() {
        // TODO
    }
}
