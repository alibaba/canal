package com.alibaba.otter.canal.prometheus.impl;

import static com.alibaba.otter.canal.prometheus.CanalInstanceExports.DEST;
import static com.alibaba.otter.canal.prometheus.CanalInstanceExports.DEST_LABELS_LIST;
import io.prometheus.client.Collector;
import io.prometheus.client.CounterMetricFamily;
import io.prometheus.client.GaugeMetricFamily;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser;
import com.alibaba.otter.canal.prometheus.InstanceRegistry;
import com.google.common.base.Preconditions;

/**
 * @author Chuanyi Li
 */
public class ParserCollector extends Collector implements InstanceRegistry {

    private static final Logger                              logger                = LoggerFactory.getLogger(ParserCollector.class);
    private static final long                                NANO_PER_MILLI        = 1000 * 1000L;
    private static final String                              PUBLISH_BLOCKING      = "canal_instance_publish_blocking_time";
    private static final String                              RECEIVED_BINLOG       = "canal_instance_received_binlog_bytes";
    private static final String                              PARSER_MODE           = "canal_instance_parser_mode";
    private static final String                              MODE_LABEL            = "parallel";
    private static final String                              PUBLISH_BLOCKING_HELP = "Publish blocking time of dump thread in milliseconds";
    private static final String                              RECEIVED_BINLOG_HELP  = "Received binlog bytes";
    private static final String                              MODE_HELP             = "Parser mode(parallel/serial) of instance";
    private final List<String>                               modeLabels            = Arrays.asList(DEST, MODE_LABEL);
    private final ConcurrentMap<String, ParserMetricsHolder> instances             = new ConcurrentHashMap<String, ParserMetricsHolder>();

    private ParserCollector() {}

    private static class SingletonHolder {
        private static final ParserCollector SINGLETON = new ParserCollector();
    }

    public static ParserCollector instance() {
        return SingletonHolder.SINGLETON;
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> mfs = new ArrayList<MetricFamilySamples>();
        boolean hasParallel = false;
        CounterMetricFamily bytesCounter = new CounterMetricFamily(RECEIVED_BINLOG,
                RECEIVED_BINLOG_HELP, DEST_LABELS_LIST);
        GaugeMetricFamily modeGauge = new GaugeMetricFamily(PARSER_MODE,
                MODE_HELP, modeLabels);
        CounterMetricFamily blockingCounter = new CounterMetricFamily(PUBLISH_BLOCKING,
                PUBLISH_BLOCKING_HELP, DEST_LABELS_LIST);
        for (ParserMetricsHolder emh : instances.values()) {
            if (emh.isParallel) {
                blockingCounter.addMetric(emh.destLabelValues, (emh.eventsPublishBlockingTime.doubleValue() / NANO_PER_MILLI));
                hasParallel = true;
            }
            modeGauge.addMetric(emh.modeLabelValues, 1);
            bytesCounter.addMetric(emh.destLabelValues, emh.receivedBinlogBytes.doubleValue());

        }
        mfs.add(bytesCounter);
        mfs.add(modeGauge);
        if (hasParallel) {
            mfs.add(blockingCounter);
        }
        return mfs;
    }

    @Override
    public void register(CanalInstance instance) {
        final String destination = instance.getDestination();
        ParserMetricsHolder holder = new ParserMetricsHolder();
        CanalEventParser parser = instance.getEventParser();
        if (!(parser instanceof MysqlEventParser)) {
            throw new IllegalArgumentException("CanalEventParser must be MysqlEventParser");
        }
        MysqlEventParser mysqlParser = (MysqlEventParser) parser;
        holder.destLabelValues = Collections.singletonList(destination);
        holder.modeLabelValues = Arrays.asList(destination, Boolean.toString(mysqlParser.isParallel()));
        holder.eventsPublishBlockingTime = mysqlParser.getEventsPublishBlockingTime();
        holder.receivedBinlogBytes = mysqlParser.getReceivedBinlogBytes();
        holder.isParallel = mysqlParser.isParallel();
        Preconditions.checkNotNull(holder.eventsPublishBlockingTime);
        Preconditions.checkNotNull(holder.receivedBinlogBytes);
        ParserMetricsHolder old = instances.put(destination, holder);
        if (old != null) {
            logger.warn("Remove stale ParserCollector for instance {}.", destination);
        }
    }

    @Override
    public void unregister(CanalInstance instance) {
        final String destination = instance.getDestination();
        instances.remove(destination);
    }

    private class ParserMetricsHolder {
        private List<String> destLabelValues;
        private List<String> modeLabelValues;
        private AtomicLong   receivedBinlogBytes;
        private AtomicLong   eventsPublishBlockingTime;
        private boolean      isParallel;
    }

}
