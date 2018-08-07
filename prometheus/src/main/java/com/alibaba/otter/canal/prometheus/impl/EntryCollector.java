package com.alibaba.otter.canal.prometheus.impl;

import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.prometheus.InstanceRegistry;
import com.alibaba.otter.canal.sink.CanalEventSink;
import com.alibaba.otter.canal.sink.entry.EntryEventSink;
import com.google.common.base.Preconditions;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.otter.canal.prometheus.CanalInstanceExports.DEST_LABEL_LIST;

/**
 * @author Chuanyi Li
 */
public class EntryCollector extends Collector implements InstanceRegistry {

    private static final Logger                             logger              = LoggerFactory.getLogger(SinkCollector.class);
    private static final String                             DELAY_NAME          = "canal_instance_traffic_delay";
    private static final String                             delayHelpName       = "Traffic delay of canal instance";
    private final ConcurrentMap<String, EntryMetricsHolder> instances           = new ConcurrentHashMap<String, EntryMetricsHolder>();

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> mfs = new ArrayList<MetricFamilySamples>();
        GaugeMetricFamily delay = new GaugeMetricFamily(DELAY_NAME,
                delayHelpName, DEST_LABEL_LIST);
        for (EntryMetricsHolder emh : instances.values()) {
            delay.addMetric(emh.destLabelValues, emh.latestExecTime.doubleValue());
        }
        mfs.add(delay);
        return mfs;
    }

    @Override
    public void register(CanalInstance instance) {
        final String destination = instance.getDestination();
        EntryMetricsHolder holder = new EntryMetricsHolder();
        holder.destLabelValues = Collections.singletonList(destination);
        CanalEventSink sink = instance.getEventSink();
        if (!(sink instanceof EntryEventSink)) {
            throw new IllegalArgumentException("CanalEventSink must be EntryEventSink");
        }
        EntryEventSink entrySink = (EntryEventSink) sink;
        PrometheusCanalEventDownStreamHandler handler = new PrometheusCanalEventDownStreamHandler();
        holder.latestExecTime = handler.getLatestExecuteTime();
        entrySink.addHandler(handler, 0);
        Preconditions.checkNotNull(holder.destLabelValues);
        Preconditions.checkNotNull(holder.latestExecTime);
        EntryMetricsHolder old = instances.putIfAbsent(destination, holder);
        if (old != null) {
            logger.warn("Ignore repeated EntryCollector register for instance {}.", destination);
        }
    }

    @Override
    public void unregister(CanalInstance instance) {
        final String destination = instance.getDestination();
        instances.remove(destination);
    }

    private class EntryMetricsHolder {
        private AtomicLong   latestExecTime;
        private List<String> destLabelValues;
    }

}
