package com.alibaba.otter.canal.prometheus.impl;

import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.prometheus.InstanceRegistry;
import com.alibaba.otter.canal.sink.CanalEventSink;
import com.alibaba.otter.canal.sink.entry.EntryEventSink;
import com.google.common.base.Preconditions;
import io.prometheus.client.Collector;
import io.prometheus.client.CounterMetricFamily;
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
public class SinkCollector extends Collector implements InstanceRegistry {

    private static final Logger                            logger             = LoggerFactory.getLogger(SinkCollector.class);
    private static final String                            SINK_BLOCKING_TIME = "canal_instance_sink_blocking_time";
    private static final String                            sinkBlockTimeHelp  = "Total sink blocking time";
    private final ConcurrentMap<String, SinkMetricsHolder> instances          = new ConcurrentHashMap<String, SinkMetricsHolder>();

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> mfs = new ArrayList<MetricFamilySamples>();
        CounterMetricFamily blockingCounter = new CounterMetricFamily(SINK_BLOCKING_TIME,
                sinkBlockTimeHelp, DEST_LABEL_LIST);
        for (SinkMetricsHolder smh : instances.values()) {
            blockingCounter.addMetric(smh.destLabelValues, smh.eventsSinkBlockingTime.doubleValue());
        }
        mfs.add(blockingCounter);
        return mfs;
    }

    @Override
    public void register(CanalInstance instance) {
        final String destination = instance.getDestination();
        SinkMetricsHolder holder = new SinkMetricsHolder();
        holder.destLabelValues = Collections.singletonList(destination);
        CanalEventSink sink = instance.getEventSink();
        if (!(sink instanceof EntryEventSink)) {
            throw new IllegalArgumentException("CanalEventSink must be EntryEventSink");
        }
        EntryEventSink entrySink = (EntryEventSink) sink;
        holder.eventsSinkBlockingTime = entrySink.getEventsSinkBlockingTime();
        Preconditions.checkNotNull(holder.destLabelValues);
        Preconditions.checkNotNull(holder.eventsSinkBlockingTime);
        SinkMetricsHolder old = instances.putIfAbsent(destination, holder);
        if (old != null) {
            logger.warn("Ignore repeated SinkCollector register for instance {}.", destination);
        }
    }

    @Override
    public void unregister(CanalInstance instance) {
        final String destination = instance.getDestination();
        instances.remove(destination);
    }

    private class SinkMetricsHolder {
        private AtomicLong   eventsSinkBlockingTime;
        private List<String> destLabelValues;
    }
}
