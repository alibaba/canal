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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.alibaba.otter.canal.prometheus.CanalInstanceExports.DEST_LABELS_LIST;

/**
 * @author Chuanyi Li
 */
public class MetaCollector extends Collector implements InstanceRegistry {

    private static final List<String>                      INFO_LABELS_LIST  = Arrays.asList("destination", "mode");
    private static final Logger                            logger            = LoggerFactory.getLogger(MetaCollector.class);
    private static final String                            INSTANCE          = "canal_instance";
    private static final String                            INSTANCE_HELP     = "Canal instance";
    private static final String                            SUBSCRIPTION      = "canal_instance_subscriptions";
    private static final String                            SUBSCRIPTION_HELP = "Canal instance subscriptions";
    private final ConcurrentMap<String, MetaMetricsHolder> instances         = new ConcurrentHashMap<>();

    private MetaCollector() {}

    private static class SingletonHolder {
        private static final MetaCollector SINGLETON = new MetaCollector();
    }

    public static MetaCollector instance() {
        return SingletonHolder.SINGLETON;
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> mfs = new ArrayList<>();
        GaugeMetricFamily instanceInfo = new GaugeMetricFamily(INSTANCE,
                INSTANCE_HELP, INFO_LABELS_LIST);
        GaugeMetricFamily subsInfo = new GaugeMetricFamily(SUBSCRIPTION,
                SUBSCRIPTION_HELP, DEST_LABELS_LIST);
        for (Map.Entry<String, MetaMetricsHolder> nme : instances.entrySet()) {
            final String destination = nme.getKey();
            final MetaMetricsHolder nmh = nme.getValue();
            instanceInfo.addMetric(nmh.infoLabelValues, 1);
            List<ClientIdentity> subs = nmh.metaManager.listAllSubscribeInfo(destination);
            int count = subs == null ? 0 : subs.size();
            subsInfo.addMetric(nmh.destLabelValues, count);
        }
        mfs.add(instanceInfo);
        mfs.add(subsInfo);
        return mfs;
    }

    @Override
    public void register(CanalInstance instance) {
        final String destination = instance.getDestination();
        MetaMetricsHolder holder = new MetaMetricsHolder();
        String mode = (instance instanceof CanalInstanceWithSpring) ? "spring" : "manager";
        holder.infoLabelValues = Arrays.asList(destination, mode);
        holder.destLabelValues = Collections.singletonList(destination);
        holder.metaManager = instance.getMetaManager();
        Preconditions.checkNotNull(holder.metaManager);
        MetaMetricsHolder old = instances.put(destination, holder);
        if (old != null) {
            logger.warn("Remove stale MetaCollector for instance {}.", destination);
        }
    }

    @Override
    public void unregister(CanalInstance instance) {
        final String destination = instance.getDestination();
        instances.remove(destination);
    }

    private static class MetaMetricsHolder {
        private List<String>     infoLabelValues;
        private List<String>     destLabelValues;
        private CanalMetaManager metaManager;
    }


}
