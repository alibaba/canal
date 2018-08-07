package com.alibaba.otter.canal.prometheus.impl;

import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.instance.spring.CanalInstanceWithSpring;
import com.alibaba.otter.canal.meta.CanalMetaManager;
import com.alibaba.otter.canal.prometheus.CanalInstanceExports;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Chuanyi Li
 */
public class InstanceMetaCollector extends Collector {

    private static final List<String> InfoLabel    = Arrays.asList("destination", "mode");

    private CanalMetaManager          metaManager;

    private final String              destination;

    private final String              mode;

    private final String              subsHelp;

    public InstanceMetaCollector(CanalInstance instance) {
        if (instance == null) {
            throw new IllegalArgumentException("CanalInstance must not be null.");
        }
        if (instance instanceof CanalInstanceWithSpring) {
            mode = "spring";
        } else {
            mode = "manager";
        }
        this.metaManager = instance.getMetaManager();
        this.destination = instance.getDestination();
        this.subsHelp = "Subscriptions of canal instance " + destination;
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> mfs = new ArrayList<MetricFamilySamples>();
        GaugeMetricFamily instanceInfo = new GaugeMetricFamily(
                "canal_instance",
                "Canal instance",
                InfoLabel);
        instanceInfo.addMetric(Arrays.asList(destination, mode), 1);
        mfs.add(instanceInfo);
        if (metaManager.isStart()) {
            // client id = hardcode 1001, 目前没有意义
            List<ClientIdentity> subs = metaManager.listAllSubscribeInfo(destination);
            GaugeMetricFamily subscriptions = new GaugeMetricFamily(
                    "canal_instance_subscription",
                    subsHelp, CanalInstanceExports.labelList);
            subscriptions.addMetric(Arrays.asList(destination), subs.size());
            mfs.add(subscriptions);
        }
        return mfs;
    }
}
