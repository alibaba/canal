package com.alibaba.otter.canal.prometheus.impl;

import com.alibaba.otter.canal.sink.CanalEventSink;
import com.alibaba.otter.canal.sink.entry.EntryEventSink;
import io.prometheus.client.Collector;
import io.prometheus.client.CounterMetricFamily;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.otter.canal.prometheus.CanalInstanceExports.DEST_LABEL_LIST;

/**
 * @author Chuanyi Li
 */
public class EntrySinkCollector extends Collector {

    private static final String       SINK_BLOCKING_TIME      = "canal_instance_sink_blocking_time";
    private final String              sinkBlockTimeHelp;
    private final List<String>        destLabelValues;
    private final AtomicLong          eventsSinkBlockingTime;


    public EntrySinkCollector(CanalEventSink sink, String destination) {
        if (!(sink instanceof EntryEventSink)) {
            throw new IllegalArgumentException("CanalEventSink must be EntryEventSink");
        }
        this.destLabelValues = Collections.singletonList(destination);
        EntryEventSink entrySink = (EntryEventSink) sink;
        this.sinkBlockTimeHelp = "Sink blocking time of instance " + destination;
        eventsSinkBlockingTime = entrySink.getEventsSinkBlockingTime();
    }

    @Override public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> mfs = new ArrayList<MetricFamilySamples>();
        CounterMetricFamily blockingCounter = new CounterMetricFamily(SINK_BLOCKING_TIME,
                sinkBlockTimeHelp, DEST_LABEL_LIST);
        blockingCounter.addMetric(destLabelValues, eventsSinkBlockingTime.doubleValue());
        mfs.add(blockingCounter);
        return mfs;
    }

}
