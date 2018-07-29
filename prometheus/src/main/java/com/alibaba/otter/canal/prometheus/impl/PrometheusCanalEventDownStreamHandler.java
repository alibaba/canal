package com.alibaba.otter.canal.prometheus.impl;

import com.alibaba.otter.canal.prometheus.CanalInstanceExports;
import com.alibaba.otter.canal.sink.AbstractCanalEventDownStreamHandler;
import com.alibaba.otter.canal.store.model.Event;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Chuanyi Li
 */
public class PrometheusCanalEventDownStreamHandler extends AbstractCanalEventDownStreamHandler<List<Event>> {

    private final Collector     collector;

    private long                latestExecuteTime = 0L;

    private static final String DELAY_NAME        = "canal_instance_traffic_delay";

    private final String        delayHelpName;

    private final List<String>  labelValues;

    public PrometheusCanalEventDownStreamHandler(final String destination) {
        this.delayHelpName = "Traffic delay of canal instance " + destination + " in seconds.";
        this.labelValues = Collections.singletonList(destination);
        collector = new Collector() {
            @Override
            public List<MetricFamilySamples> collect() {
                List<MetricFamilySamples> mfs = new ArrayList<MetricFamilySamples>();
                long now = System.currentTimeMillis();
                GaugeMetricFamily delay = new GaugeMetricFamily(
                        DELAY_NAME,
                        delayHelpName,
                        CanalInstanceExports.labelList);
                double d = 0.0;
                if (latestExecuteTime > 0) {
                    d = now - latestExecuteTime;
                }
                d = d > 0.0 ? (d / 1000) : 0.0;
                delay.addMetric(labelValues, d);
                mfs.add(delay);
                return mfs;
            }
        };
    }

    @Override
    public List<Event> before(List<Event> events) {
        // TODO utilize MySQL master heartbeat packet to refresh delay if always no more events coming
        // see: https://dev.mysql.com/worklog/task/?id=342
        // heartbeats are sent by the master only if there is no
        // more unsent events in the actual binlog file for a period longer that
        // master_heartbeat_period.
        if (events != null && !events.isEmpty()) {
            Event last = events.get(events.size() - 1);
            long ts = last.getExecuteTime();
            if (ts > latestExecuteTime) {
                latestExecuteTime = ts;
            }
        }
        return events;
    }

    public Collector getCollector() {
        return this.collector;
    }

}
