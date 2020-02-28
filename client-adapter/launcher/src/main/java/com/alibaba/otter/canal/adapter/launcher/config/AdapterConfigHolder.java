package com.alibaba.otter.canal.adapter.launcher.config;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.otter.canal.adapter.launcher.monitor.remote.ConfigItem;

/**
 * @author L.J.R @ 2019-09-03
 * @version 1.1.4
 */
public class AdapterConfigHolder {

    private volatile long adapterConfigTimestamp = 0;

    private final Map<String, ConfigItem> adapterConfigs = new ConcurrentHashMap<>();

    private static AdapterConfigHolder adapterConfigHolder;

    private AdapterConfigHolder() {
    }

    public synchronized static AdapterConfigHolder getInstance() {
        if (adapterConfigHolder == null) {
            adapterConfigHolder = new AdapterConfigHolder();
        }
        return adapterConfigHolder;
    }

    public void setAdapterConfigTimestamp(long configTimestamp) {
        this.adapterConfigTimestamp = configTimestamp;
    }

    public long getAdapterConfigTimestamp() {
        return adapterConfigTimestamp;
    }

    public Map<String, ConfigItem> getAdapterConfigs() {
        return this.adapterConfigs;
    }
}
