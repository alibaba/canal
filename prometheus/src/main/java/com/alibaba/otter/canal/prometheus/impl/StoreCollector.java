package com.alibaba.otter.canal.prometheus.impl;

import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.prometheus.CanalInstanceExports;
import com.alibaba.otter.canal.prometheus.InstanceRegistry;
import com.alibaba.otter.canal.store.CanalEventStore;
import com.alibaba.otter.canal.store.CanalStoreException;
import com.alibaba.otter.canal.store.memory.MemoryEventStoreWithBuffer;
import io.prometheus.client.Collector;
import io.prometheus.client.CounterMetricFamily;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Chuanyi Li
 */
public class StoreCollector extends Collector implements InstanceRegistry {

    private static final Class<MemoryEventStoreWithBuffer> clazz  = MemoryEventStoreWithBuffer.class;
    private static final String                             PRODUCE     = "canal_instance_store_produce_seq";
    private static final String                             CONSUME     = "canal_instance_store_consume_seq";
    private static final String                             produceHelp = "Produced sequence of canal instance";
    private static final String                             consumeHelp = "Consumed sequence of canal instance";
    private final ConcurrentMap<String, StoreMetricsHolder> instances   = new ConcurrentHashMap<String, StoreMetricsHolder>();


    public StoreCollector(CanalEventStore store, String destination) {
        this.destination = destination;
        if (!(store instanceof MemoryEventStoreWithBuffer)) {
            throw new IllegalArgumentException("EventStore must be MemoryEventStoreWithBuffer");
        }
        MemoryEventStoreWithBuffer ms = (MemoryEventStoreWithBuffer) store;
        putSequence = getDeclaredValue(ms, "putSequence");
        ackSequence = getDeclaredValue(ms, "ackSequence");
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> mfs = new ArrayList<MetricFamilySamples>();
        CounterMetricFamily put = new CounterMetricFamily("canal_instance_store_produce_seq",
                putHelp, Arrays.asList(CanalInstanceExports.DEST_LABELS));
        put.addMetric(Collections.singletonList(destination), putSequence.doubleValue());
        mfs.add(put);
        CounterMetricFamily ack = new CounterMetricFamily("canal_instance_store_consume_seq",
                ackHelp, Arrays.asList(CanalInstanceExports.DEST_LABELS));
        ack.addMetric(Collections.singletonList(destination), ackSequence.doubleValue());
        mfs.add(ack);
        return mfs;
    }

    @SuppressWarnings("unchecked")
    private static <T> T getDeclaredValue(MemoryEventStoreWithBuffer store, String name) {
        T value;
        try {
            Field putField = clazz.getDeclaredField(name);
            putField.setAccessible(true);
            value = (T) putField.get(store);
        } catch (NoSuchFieldException e) {
            throw new CanalStoreException(e);
        } catch (IllegalAccessException e) {
            throw new CanalStoreException(e);
        }
        return value;
    }

    @Override public void register(CanalInstance instance) {

    }

    @Override public void unregister(CanalInstance instance) {

    }

    private class StoreMetricsHolder {
        private AtomicLong putSeq;
        private AtomicLong ackSeq;
    }
}
