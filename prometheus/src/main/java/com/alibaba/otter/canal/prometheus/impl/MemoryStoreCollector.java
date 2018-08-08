package com.alibaba.otter.canal.prometheus.impl;

import com.alibaba.otter.canal.prometheus.CanalInstanceExports;
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
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Chuanyi Li
 */
public class MemoryStoreCollector extends Collector {

    private static final Class<MemoryEventStoreWithBuffer> clazz  = MemoryEventStoreWithBuffer.class;

    private final String                                   destination;

    private final AtomicLong                               putSequence;

    private final AtomicLong                               ackSequence;

    private final String                                   putHelp;

    private final String                                   ackHelp;

    public MemoryStoreCollector(CanalEventStore store, String destination) {
        this.destination = destination;
        if (!(store instanceof MemoryEventStoreWithBuffer)) {
            throw new IllegalArgumentException("EventStore must be MemoryEventStoreWithBuffer");
        }
        MemoryEventStoreWithBuffer ms = (MemoryEventStoreWithBuffer) store;
        putSequence = getDeclaredValue(ms, "putSequence");
        ackSequence = getDeclaredValue(ms, "ackSequence");
        putHelp = "Produced sequence of canal instance " + destination;
        ackHelp = "Consumed sequence of canal instance " + destination;
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> mfs = new ArrayList<MetricFamilySamples>();
        CounterMetricFamily put = new CounterMetricFamily("canal_instance_store_produce_seq",
                putHelp, Arrays.asList(CanalInstanceExports.labels));
        put.addMetric(Collections.singletonList(destination), putSequence.doubleValue());
        mfs.add(put);
        CounterMetricFamily ack = new CounterMetricFamily("canal_instance_store_consume_seq",
                ackHelp, Arrays.asList(CanalInstanceExports.labels));
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

}
