package com.alibaba.otter.canal.prometheus.impl;

import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.prometheus.InstanceRegistry;
import com.alibaba.otter.canal.store.CanalEventStore;
import com.alibaba.otter.canal.store.memory.MemoryEventStoreWithBuffer;
import com.alibaba.otter.canal.store.model.BatchMode;
import com.google.common.base.Preconditions;
import io.prometheus.client.Collector;
import io.prometheus.client.CounterMetricFamily;
import io.prometheus.client.GaugeMetricFamily;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.otter.canal.prometheus.CanalInstanceExports.DEST;
import static com.alibaba.otter.canal.prometheus.CanalInstanceExports.DEST_LABELS_LIST;

/**
 * @author Chuanyi Li
 */
public class StoreCollector extends Collector implements InstanceRegistry {

    private static final Logger                             logger           = LoggerFactory.getLogger(SinkCollector.class);
    private static final String                             PRODUCE          = "canal_instance_store_produce_seq";
    private static final String                             CONSUME          = "canal_instance_store_consume_seq";
    private static final String                             STORE            = "canal_instance_store";
    private static final String                             PRODUCE_MEM      = "canal_instance_store_produce_mem";
    private static final String                             CONSUME_MEM      = "canal_instance_store_consume_mem";
    private static final String                             PUT_DELAY        = "canal_instance_put_delay";
    private static final String                             GET_DELAY        = "canal_instance_get_delay";
    private static final String                             ACK_DELAY        = "canal_instance_ack_delay";
    private static final String                             PUT_ROWS         = "canal_instance_put_rows";
    private static final String                             GET_ROWS         = "canal_instance_get_rows";
    private static final String                             ACK_ROWS         = "canal_instance_ack_rows";
    private static final String                             PRODUCE_HELP     = "Produced events counter of canal instance";
    private static final String                             CONSUME_HELP     = "Consumed events counter of canal instance";
    private static final String                             STORE_HELP       = "Canal instance info";
    private static final String                             PRODUCE_MEM_HELP = "Produced mem bytes of canal instance";
    private static final String                             CONSUME_MEM_HELP = "Consumed mem bytes of canal instance";
    private static final String                             PUT_DELAY_HELP   = "Traffic delay of canal instance put";
    private static final String                             GET_DELAY_HELP   = "Traffic delay of canal instance get";
    private static final String                             ACK_DELAY_HELP   = "Traffic delay of canal instance ack";
    private static final String                             PUT_ROWS_HELP    = "Put table rows of canal instance";
    private static final String                             GET_ROWS_HELP    = "Got table rows of canal instance";
    private static final String                             ACK_ROWS_HELP    = "Acked table rows of canal instance";
    private final ConcurrentMap<String, StoreMetricsHolder> instances        = new ConcurrentHashMap<>();
    private final List<String>                              storeLabelsList  = Arrays.asList(DEST, "batchMode", "size");

    private StoreCollector() {}

    private static class SingletonHolder {
        private static final StoreCollector SINGLETON = new StoreCollector();
    }

    public static StoreCollector instance() {
        return SingletonHolder.SINGLETON;
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> mfs = new ArrayList<>();
        CounterMetricFamily put = new CounterMetricFamily(PRODUCE,
                PRODUCE_HELP, DEST_LABELS_LIST);
        CounterMetricFamily ack = new CounterMetricFamily(CONSUME,
                CONSUME_HELP, DEST_LABELS_LIST);
        GaugeMetricFamily store = new GaugeMetricFamily(STORE,
                STORE_HELP, storeLabelsList);
        CounterMetricFamily putMem = new CounterMetricFamily(PRODUCE_MEM,
                PRODUCE_MEM_HELP, DEST_LABELS_LIST);
        CounterMetricFamily ackMem = new CounterMetricFamily(CONSUME_MEM,
                CONSUME_MEM_HELP, DEST_LABELS_LIST);
        GaugeMetricFamily putDelay = new GaugeMetricFamily(PUT_DELAY,
                PUT_DELAY_HELP, DEST_LABELS_LIST);
        GaugeMetricFamily getDelay = new GaugeMetricFamily(GET_DELAY,
                GET_DELAY_HELP, DEST_LABELS_LIST);
        GaugeMetricFamily ackDelay = new GaugeMetricFamily(ACK_DELAY,
                ACK_DELAY_HELP, DEST_LABELS_LIST);
        CounterMetricFamily putRows = new CounterMetricFamily(PUT_ROWS,
                PUT_ROWS_HELP, DEST_LABELS_LIST);
        CounterMetricFamily getRows = new CounterMetricFamily(GET_ROWS,
                GET_ROWS_HELP, DEST_LABELS_LIST);
        CounterMetricFamily ackRows = new CounterMetricFamily(ACK_ROWS,
                ACK_ROWS_HELP, DEST_LABELS_LIST);
        boolean hasMem = false;
        for (StoreMetricsHolder smh : instances.values()) {
            final boolean isMem = smh.batchMode.isMemSize();
            put.addMetric(smh.destLabelValues, smh.putSeq.doubleValue());
            ack.addMetric(smh.destLabelValues, smh.ackSeq.doubleValue());
            long pet = smh.putExecTime.get();
            // 防止出现启动时，未消费造成的get, ack延时小于前阶段的情况
            long get = Math.min(smh.getExecTime.get(), pet);
            long aet = Math.min(smh.ackExecTime.get(), get);
            long now = System.currentTimeMillis();
            // execTime > now，delay显示为0
            long pd = (now >= pet) ? (now - pet) : 0;
            putDelay.addMetric(smh.destLabelValues, pd);
            long gd = (now >= get) ? (now - get) : 0;
            getDelay.addMetric(smh.destLabelValues, gd);
            long ad = (now >= aet) ? (now - aet) : 0;
            ackDelay.addMetric(smh.destLabelValues, ad);
            putRows.addMetric(smh.destLabelValues, smh.putTableRows.doubleValue());
            getRows.addMetric(smh.destLabelValues, smh.getTableRows.doubleValue());
            ackRows.addMetric(smh.destLabelValues, smh.ackTableRows.doubleValue());
            store.addMetric(smh.storeLabelValues, 1);
            if (isMem) {
                hasMem = true;
                putMem.addMetric(smh.destLabelValues, smh.putMemSize.doubleValue());
                ackMem.addMetric(smh.destLabelValues, smh.ackMemSize.doubleValue());
            }
        }
        mfs.add(put);
        mfs.add(ack);
        mfs.add(store);
        mfs.add(putDelay);
        mfs.add(getDelay);
        mfs.add(ackDelay);
        mfs.add(putRows);
        mfs.add(getRows);
        mfs.add(ackRows);
        if (hasMem) {
            mfs.add(putMem);
            mfs.add(ackMem);
        }
        return mfs;
    }

    @Override
    public void register(CanalInstance instance) {
        final String destination = instance.getDestination();
        StoreMetricsHolder holder = new StoreMetricsHolder();
        CanalEventStore store = instance.getEventStore();
        if (!(store instanceof MemoryEventStoreWithBuffer)) {
            throw new IllegalArgumentException("EventStore must be MemoryEventStoreWithBuffer");
        }
        MemoryEventStoreWithBuffer memStore = (MemoryEventStoreWithBuffer) store;
        holder.batchMode = memStore.getBatchMode();
        holder.putSeq = memStore.getPutSequence();
        holder.ackSeq = memStore.getAckSequence();
        holder.destLabelValues = Collections.singletonList(destination);
        holder.size = memStore.getBufferSize();
        holder.storeLabelValues = Arrays.asList(destination, holder.batchMode.name(), Integer.toString(holder.size));
        holder.putExecTime = memStore.getPutExecTime();
        holder.getExecTime = memStore.getGetExecTime();
        holder.ackExecTime = memStore.getAckExecTime();
        holder.putTableRows = memStore.getPutTableRows();
        holder.getTableRows = memStore.getGetTableRows();
        holder.ackTableRows = memStore.getAckTableRows();
        Preconditions.checkNotNull(holder.batchMode);
        Preconditions.checkNotNull(holder.putSeq);
        Preconditions.checkNotNull(holder.ackSeq);
        if (holder.batchMode.isMemSize()) {
            holder.putMemSize = memStore.getPutMemSize();
            holder.ackMemSize = memStore.getAckMemSize();
            Preconditions.checkNotNull(holder.putMemSize);
            Preconditions.checkNotNull(holder.ackMemSize);
        }
        StoreMetricsHolder old = instances.putIfAbsent(destination, holder);
        if (old != null) {
            logger.warn("Remote stale StoreCollector for instance {}.", destination);
        }
    }

    @Override
    public void unregister(CanalInstance instance) {
        final String destination = instance.getDestination();
        instances.remove(destination);
    }

    private static class StoreMetricsHolder {
        private AtomicLong   putSeq;
        private AtomicLong   ackSeq;
        private BatchMode    batchMode;
        private AtomicLong   putMemSize;
        private AtomicLong   ackMemSize;
        private AtomicLong   putExecTime;
        private AtomicLong   getExecTime;
        private AtomicLong   ackExecTime;
        private AtomicLong   putTableRows;
        private AtomicLong   getTableRows;
        private AtomicLong   ackTableRows;
        private int          size;
        private List<String> destLabelValues;
        private List<String> storeLabelValues;
    }
}
