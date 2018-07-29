package com.alibaba.otter.canal.prometheus;

import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.prometheus.impl.InstanceMetaCollector;
import com.alibaba.otter.canal.prometheus.impl.MemoryStoreCollector;
import com.alibaba.otter.canal.prometheus.impl.PrometheusCanalEventDownStreamHandler;
import com.alibaba.otter.canal.sink.CanalEventSink;
import com.alibaba.otter.canal.sink.entry.EntryEventSink;
import com.alibaba.otter.canal.store.CanalStoreException;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * @author Chuanyi Li
 */
public class CanalInstanceExports {

    private static final Logger      logger         = LoggerFactory.getLogger(CanalInstanceExports.class);

    public static final String[]     labels         = {"destination"};

    public static final List<String> labelList      = Collections.singletonList(labels[0]);

    private final String             destination;

    private Collector                storeCollector;

    private Collector                delayCollector;

    private Collector                metaCollector;

    private CanalInstanceExports(CanalInstance instance) {
        this.destination = instance.getDestination();
        initDelayGauge(instance);
        initStoreCollector(instance);
        initMetaCollector(instance);
    }



    static CanalInstanceExports forInstance(CanalInstance instance) {
        return new CanalInstanceExports(instance);
    }

    void register() {
        if (delayCollector != null) {
            delayCollector.register();
        }
        if (storeCollector != null) {
            storeCollector.register();
        }
        if (metaCollector != null) {
            metaCollector.register();
        }
    }

    void unregister() {
        if (delayCollector != null) {
            CollectorRegistry.defaultRegistry.unregister(delayCollector);
        }
        if (storeCollector != null) {
            CollectorRegistry.defaultRegistry.unregister(storeCollector);
        }
        if (metaCollector != null) {
            CollectorRegistry.defaultRegistry.unregister(metaCollector);
        }
    }

    private void initDelayGauge(CanalInstance instance) {
        CanalEventSink sink = instance.getEventSink();
        if (sink instanceof EntryEventSink) {
            EntryEventSink entryEventSink = (EntryEventSink) sink;
            // TODO ensure not to add handler again
            PrometheusCanalEventDownStreamHandler handler = new PrometheusCanalEventDownStreamHandler(destination);
            entryEventSink.addHandler(handler);
            delayCollector = handler.getCollector();
        } else {
            logger.warn("This impl register metrics for only EntryEventSink, skip.");
        }
    }

    private void initStoreCollector(CanalInstance instance) {
        try {
            storeCollector = new MemoryStoreCollector(instance.getEventStore(), destination);
        } catch (CanalStoreException cse) {
            logger.warn("Failed to register metrics for destination {}.", destination, cse);
        }
    }

    private void initMetaCollector(CanalInstance instance) {
        metaCollector = new InstanceMetaCollector(instance);
    }
}
