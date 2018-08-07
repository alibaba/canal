package com.alibaba.otter.canal.prometheus.impl;

import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.instance.spring.CanalInstanceWithSpring;
import com.alibaba.otter.canal.meta.CanalMetaManager;
import com.alibaba.otter.canal.prometheus.InstanceRegistry;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.google.common.base.Preconditions;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Chuanyi Li
 */
public class MetaCollector extends Collector implements InstanceRegistry {

    private static final List<String>                      InfoLabel        = Arrays.asList("destination", "mode");
    private static final Logger                            logger           = LoggerFactory.getLogger(MetaCollector.class);
    private static final String                            SUBSCRIPTION     = "canal_instance";
    private static final String                            subscriptionHelp = "Canal instance";
    private final ConcurrentMap<String, MetaMetricsHolder> instances        = new ConcurrentHashMap<String, MetaMetricsHolder>();

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> mfs = new ArrayList<MetricFamilySamples>();
        GaugeMetricFamily instanceInfo = new GaugeMetricFamily(
                SUBSCRIPTION,
                subscriptionHelp,
                InfoLabel);
        for (Map.Entry<String, MetaMetricsHolder> nme : instances.entrySet()) {
            final String destination = nme.getKey();
            final MetaMetricsHolder nmh = nme.getValue();
            List<ClientIdentity> subs = nmh.metaManager.listAllSubscribeInfo(destination);
            int count = subs == null ? 0 : subs.size();
            instanceInfo.addMetric(nmh.infoLabelValues, count);
        }
        mfs.add(instanceInfo);
        return mfs;
    }

    @Override
    public void register(CanalInstance instance) {
        final String destination = instance.getDestination();
        MetaMetricsHolder holder = new MetaMetricsHolder();
        String mode = (instance instanceof CanalInstanceWithSpring) ? "spring" : "manager";
        holder.infoLabelValues = Arrays.asList(destination, mode);
        holder.metaManager = instance.getMetaManager();
        Preconditions.checkNotNull(holder.infoLabelValues);
        Preconditions.checkNotNull(holder.metaManager);
        MetaMetricsHolder old = instances.putIfAbsent(destination, holder);
        if (old != null) {
            logger.warn("Ignore repeated MetaCollector register for instance {}.", destination);
        }
    }

    @Override
    public void unregister(CanalInstance instance) {
        final String destination = instance.getDestination();
        instances.remove(destination);
    }

    private class MetaMetricsHolder {
        private List<String>     infoLabelValues;
        private CanalMetaManager metaManager;
    }


}
