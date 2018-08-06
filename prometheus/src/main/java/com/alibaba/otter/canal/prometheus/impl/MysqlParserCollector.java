package com.alibaba.otter.canal.prometheus.impl;

import com.alibaba.otter.canal.common.utils.SerializedLongAdder;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser;
import io.prometheus.client.Collector;
import io.prometheus.client.CounterMetricFamily;
import io.prometheus.client.GaugeMetricFamily;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.alibaba.otter.canal.prometheus.CanalInstanceExports.DESTINATION;
import static com.alibaba.otter.canal.prometheus.CanalInstanceExports.DEST_LABEL_LIST;

/**
 * @author Chuanyi Li
 */
public class MysqlParserCollector extends Collector {

    private static final String       PUBLISH_BLOCKING_TIME = "canal_instance_publish_blocking_time";
    private static final String       RECEIVED_BINLOG_BYTES = "canal_instance_received_binlog_bytes";
    private static final String       PARSER_MODE           = "canal_instance_parser_mode";
    private static final String       MODE_LABEL            = "parallel";
    private final List<String>        destLabelValues;
    private final List<String>        modeLabelValues;
    private final SerializedLongAdder receivedBinlogBytes;
    private final SerializedLongAdder eventsPublishBlockingTime;
    private final String              publishBlockingTimeHelp;
    private final String              receivedBinlogBytesHelp;
    private final String              modeHelp;
    private final List<String>        modeLabels;
    private final Boolean             isParallel;

    public MysqlParserCollector(CanalEventParser parser, String destination) {
        if (!(parser instanceof MysqlEventParser)) {
            throw new IllegalArgumentException("CanalEventParser must be MysqlEventParser");
        }
        this.destLabelValues = Collections.singletonList(destination);
        this.eventsPublishBlockingTime = ((MysqlEventParser)parser).getEventsPublishBlockingTime();
        this.publishBlockingTimeHelp = "Blocking time of publishing of instance " + destination;
        this.receivedBinlogBytes = ((MysqlEventParser)parser).getReceivedBinlogBytes();
        this.receivedBinlogBytesHelp = "Received binlog bytes of instance" + destination;
        this.isParallel = ((MysqlEventParser)parser).isParallel();
        this.modeHelp = "Parser mode of instance" + destination;
        this.modeLabels = Arrays.asList(DESTINATION, MODE_LABEL);
        this.modeLabelValues = Arrays.asList(destination, isParallel.toString());
    }

    @Override public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> mfs = new ArrayList<MetricFamilySamples>();
        // canal_instance_received_binlog_bytes
        CounterMetricFamily bytesCounter = new CounterMetricFamily(RECEIVED_BINLOG_BYTES,
                receivedBinlogBytesHelp, DEST_LABEL_LIST);
        bytesCounter.addMetric(destLabelValues, receivedBinlogBytes.get());
        mfs.add(bytesCounter);
        // canal_instance_parser_mode
        GaugeMetricFamily modeGauge = new GaugeMetricFamily(PARSER_MODE,
                modeHelp,
                modeLabels);
        modeGauge.addMetric(modeLabelValues, 1);
        mfs.add(modeGauge);
        // canal_instance_publish_blocking_time
        if (isParallel) {
            CounterMetricFamily blockingCounter = new CounterMetricFamily(PUBLISH_BLOCKING_TIME,
                    publishBlockingTimeHelp, DEST_LABEL_LIST);
            blockingCounter.addMetric(destLabelValues, eventsPublishBlockingTime.get());
            mfs.add(blockingCounter);
        }
        return mfs;
    }
}
