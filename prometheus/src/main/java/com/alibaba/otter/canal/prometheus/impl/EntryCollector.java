package com.alibaba.otter.canal.prometheus.impl;

import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.prometheus.InstanceRegistry;
import com.alibaba.otter.canal.sink.CanalEventDownStreamHandler;
import com.alibaba.otter.canal.sink.CanalEventSink;
import com.alibaba.otter.canal.sink.entry.EntryEventSink;
import com.google.common.base.Preconditions;
import io.prometheus.client.Collector;
import io.prometheus.client.CounterMetricFamily;
import io.prometheus.client.GaugeMetricFamily;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.otter.canal.prometheus.CanalInstanceExports.DEST_LABELS_LIST;

/**
 * @author Chuanyi Li
 */
public class EntryCollector extends Collector implements InstanceRegistry {

    private static final Logger                             logger            = LoggerFactory.getLogger(SinkCollector.class);
    private static final String                             DELAY             = "canal_instance_traffic_delay";
    private static final String                             TRANSACTION       = "canal_instance_transactions";
    private static final String                             DELAY_HELP        = "Traffic delay of canal instance in milliseconds";
    private static final String                             TRANSACTION_HELP  = "Transactions counter of canal instance";
    private final ConcurrentMap<String, EntryMetricsHolder> instances        = new ConcurrentHashMap<>();

    private EntryCollector() {}

    private static class SingletonHolder {
        private static final EntryCollector SINGLETON = new EntryCollector();
    }

    public static EntryCollector instance() {
        return SingletonHolder.SINGLETON;
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> mfs = new ArrayList<>();
        GaugeMetricFamily delay = new GaugeMetricFamily(DELAY,
                DELAY_HELP, DEST_LABELS_LIST);
        CounterMetricFamily transactions = new CounterMetricFamily(TRANSACTION,
                TRANSACTION_HELP, DEST_LABELS_LIST);
        for (EntryMetricsHolder emh : instances.values()) {
            long now = System.currentTimeMillis();
            long latest = emh.latestExecTime.get();
            // execTime > now，delay显示为0
            long d = (now >= latest) ? (now - latest) : 0;
            delay.addMetric(emh.destLabelValues, d);
            transactions.addMetric(emh.destLabelValues, emh.transactionCounter.doubleValue());
        }
        mfs.add(delay);
        mfs.add(transactions);
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
        PrometheusCanalEventDownStreamHandler handler = assembleHandler(entrySink);
        holder.latestExecTime = handler.getLatestExecuteTime();
        holder.transactionCounter = handler.getTransactionCounter();
        Preconditions.checkNotNull(holder.latestExecTime);
        Preconditions.checkNotNull(holder.transactionCounter);
        EntryMetricsHolder old = instances.put(destination, holder);
        if (old != null) {
            logger.warn("Remove stale EntryCollector for instance {}.", destination);
        }
    }

    @Override
    public void unregister(CanalInstance instance) {
        final String destination = instance.getDestination();
        CanalEventSink sink = instance.getEventSink();
        if (!(sink instanceof EntryEventSink)) {
            throw new IllegalArgumentException("CanalEventSink must be EntryEventSink");
        }
        unloadHandler((EntryEventSink) sink);
        instances.remove(destination);
    }

    private PrometheusCanalEventDownStreamHandler assembleHandler(EntryEventSink entrySink) {
        PrometheusCanalEventDownStreamHandler ph = new PrometheusCanalEventDownStreamHandler();
        List<CanalEventDownStreamHandler> handlers = entrySink.getHandlers();
        for (CanalEventDownStreamHandler handler : handlers) {
            if (handler instanceof PrometheusCanalEventDownStreamHandler) {
                throw new IllegalStateException("PrometheusCanalEventDownStreamHandler already exists in handlers.");
            }
        }
        entrySink.addHandler(ph, 0);
        return ph;
    }

    private void unloadHandler(EntryEventSink entrySink) {
        List<CanalEventDownStreamHandler> handlers = entrySink.getHandlers();
        int i = 0;
        for (; i < handlers.size(); i++) {
            if (handlers.get(i) instanceof PrometheusCanalEventDownStreamHandler) {
                break;
            }
        }
        entrySink.removeHandler(i);
        // Ensure no PrometheusCanalEventDownStreamHandler
        handlers = entrySink.getHandlers();
        for (CanalEventDownStreamHandler handler : handlers) {
            if (handler instanceof PrometheusCanalEventDownStreamHandler) {
                throw new IllegalStateException("Multiple prometheusCanalEventDownStreamHandler exists in handlers.");
            }
        }
    }

    private static class EntryMetricsHolder {
        private AtomicLong   latestExecTime;
        private AtomicLong   transactionCounter;
        private List<String> destLabelValues;
    }

}
