package com.alibaba.otter.canal.prometheus.impl;

import static com.alibaba.otter.canal.prometheus.CanalInstanceExports.DEST;

import com.alibaba.otter.canal.parse.inbound.group.GroupEventParser;
import com.alibaba.otter.canal.parse.inbound.mysql.AbstractMysqlEventParser;
import io.prometheus.client.Collector;
import io.prometheus.client.CounterMetricFamily;
import io.prometheus.client.GaugeMetricFamily;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.parse.CanalEventParser;
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
    private static final String                              PARSER_LABEL          = "parser";
    private static final String                              PUBLISH_BLOCKING_HELP = "Publish blocking time of dump thread in milliseconds";
    private static final String                              RECEIVED_BINLOG_HELP  = "Received binlog bytes";
    private static final String                              MODE_HELP             = "Parser mode(parallel/serial) of instance";
    private final List<String>                               modeLabels            = Arrays.asList(DEST, MODE_LABEL);
    private final List<String>                               parserLabels          = Arrays.asList(DEST, PARSER_LABEL);
    private final ConcurrentMap<String, ParserMetricsHolder> instances             = new ConcurrentHashMap<>();

    private ParserCollector() {}

    private static class SingletonHolder {
        private static final ParserCollector SINGLETON = new ParserCollector();
    }

    public static ParserCollector instance() {
        return SingletonHolder.SINGLETON;
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> mfs = new ArrayList<>();
        CounterMetricFamily bytesCounter = new CounterMetricFamily(RECEIVED_BINLOG,
                RECEIVED_BINLOG_HELP, parserLabels);
        GaugeMetricFamily modeGauge = new GaugeMetricFamily(PARSER_MODE,
                MODE_HELP, modeLabels);
        CounterMetricFamily blockingCounter = new CounterMetricFamily(PUBLISH_BLOCKING,
                PUBLISH_BLOCKING_HELP, parserLabels);
        for (ParserMetricsHolder emh : instances.values()) {
            if (emh instanceof GroupParserMetricsHolder) {
                GroupParserMetricsHolder group = (GroupParserMetricsHolder) emh;
                for (ParserMetricsHolder semh :  group.holders) {
                    singleCollect(bytesCounter, blockingCounter, modeGauge, semh);
                }
            }
            else {
                singleCollect(bytesCounter, blockingCounter, modeGauge, emh);
            }
        }
        mfs.add(bytesCounter);
        mfs.add(modeGauge);
        if (!blockingCounter.samples.isEmpty()) {
            mfs.add(blockingCounter);
        }
        return mfs;
    }

    private void singleCollect(CounterMetricFamily bytesCounter, CounterMetricFamily blockingCounter, GaugeMetricFamily modeGauge, ParserMetricsHolder holder) {
        if (holder.isParallel) {
            blockingCounter.addMetric(holder.parserLabelValues, (holder.eventsPublishBlockingTime.doubleValue() / NANO_PER_MILLI));
        }
        modeGauge.addMetric(holder.modeLabelValues, 1);
        bytesCounter.addMetric(holder.parserLabelValues, holder.receivedBinlogBytes.doubleValue());
    }

    @Override
    public void register(CanalInstance instance) {
        final String destination = instance.getDestination();
        ParserMetricsHolder holder;
        CanalEventParser parser = instance.getEventParser();
        if (parser instanceof AbstractMysqlEventParser) {
            holder = singleHolder(destination, (AbstractMysqlEventParser)parser, "0");
        } else if (parser instanceof GroupEventParser) {
            holder = groupHolder(destination, (GroupEventParser)parser);
        } else {
            throw new IllegalArgumentException("CanalEventParser must be either AbstractMysqlEventParser or GroupEventParser.");
        }
        Preconditions.checkNotNull(holder);
        ParserMetricsHolder old = instances.put(destination, holder);
        if (old != null) {
            logger.warn("Remove stale ParserCollector for instance {}.", destination);
        }
    }

    private ParserMetricsHolder singleHolder(String destination, AbstractMysqlEventParser parser, String id) {
        ParserMetricsHolder holder = new ParserMetricsHolder();
        holder.parserLabelValues = Arrays.asList(destination, id);
        holder.modeLabelValues = Arrays.asList(destination, Boolean.toString(parser.isParallel()));
        holder.eventsPublishBlockingTime = parser.getEventsPublishBlockingTime();
        holder.receivedBinlogBytes = parser.getReceivedBinlogBytes();
        holder.isParallel = parser.isParallel();
        Preconditions.checkNotNull(holder.eventsPublishBlockingTime);
        Preconditions.checkNotNull(holder.receivedBinlogBytes);
        return holder;
    }

    private GroupParserMetricsHolder groupHolder(String destination, GroupEventParser group) {
        List<CanalEventParser> parsers = group.getEventParsers();
        GroupParserMetricsHolder groupHolder = new GroupParserMetricsHolder();
        int num = parsers.size();
        for (int i = 0; i < num; i ++) {
            CanalEventParser parser = parsers.get(i);
            if (parser instanceof AbstractMysqlEventParser) {
                ParserMetricsHolder single = singleHolder(destination, (AbstractMysqlEventParser)parser, Integer.toString(i + 1));
                groupHolder.holders.add(single);
            } else {
                logger.warn("Null or non AbstractMysqlEventParser, ignore.");
            }
        }
        return groupHolder;
    }

    @Override
    public void unregister(CanalInstance instance) {
        final String destination = instance.getDestination();
        instances.remove(destination);
    }

    private static class ParserMetricsHolder {
        private List<String> parserLabelValues;
        private List<String> modeLabelValues;
        // metrics for single parser
        private AtomicLong   receivedBinlogBytes;
        private AtomicLong   eventsPublishBlockingTime;
        // parser mode
        private boolean      isParallel;
    }

    private static class GroupParserMetricsHolder extends ParserMetricsHolder {
        private final List<ParserMetricsHolder> holders = new ArrayList<>();
    }

}
