package com.alibaba.otter.canal.prometheus;

import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.prometheus.impl.*;
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

    private static final Logger      logger          = LoggerFactory.getLogger(CanalInstanceExports.class);
    public static final String       DEST            = "destination";
    public static final String[]     DEST_LABELS     = {DEST};
    public static final List<String> DEST_LABEL_LIST = Collections.singletonList(DEST);
//    private final String             destination;
    private Collector                storeCollector;
    private Collector                delayCollector;
    private Collector                metaCollector;
    private Collector                sinkCollector;
    private Collector                parserCollector;

    public CanalInstanceExports() {
//        this.destination = instance.getDestination();
//        initEventsMetrics(instance);
//        initStoreCollector(instance);
//        initMetaCollector(instance);
//        initSinkCollector(instance);
//        initParserCollector(instance);
    }



//    static CanalInstanceExports forInstance(CanalInstance instance) {
//        return new CanalInstanceExports(instance);
//    }

    void register(CanalInstance instance) {
        if (delayCollector != null) {
            delayCollector.register();
        }
        if (storeCollector != null) {
            storeCollector.register();
        }
        if (metaCollector != null) {
            metaCollector.register();
        }
        if (sinkCollector != null) {
            sinkCollector.register();
        }
        if (parserCollector != null) {
            parserCollector.register();
        }
    }

    void unregister(CanalInstance instance) {
        if (delayCollector != null) {
            CollectorRegistry.defaultRegistry.unregister(delayCollector);
        }
        if (storeCollector != null) {
            CollectorRegistry.defaultRegistry.unregister(storeCollector);
        }
        if (metaCollector != null) {
            CollectorRegistry.defaultRegistry.unregister(metaCollector);
        }
        if (sinkCollector != null) {
            CollectorRegistry.defaultRegistry.unregister(sinkCollector);
        }
        if (parserCollector != null) {
            CollectorRegistry.defaultRegistry.unregister(parserCollector);
        }
    }

    private void initEventsMetrics(CanalInstance instance) {
        CanalEventSink sink = instance.getEventSink();
        if (sink instanceof EntryEventSink) {
            EntryEventSink entryEventSink = (EntryEventSink) sink;
            // TODO ensure not to add handler again
//            PrometheusCanalEventDownStreamHandler handler = new PrometheusCanalEventDownStreamHandler(destination);
//            entryEventSink.addHandler(handler);
//            delayCollector = handler.getCollector();
        } else {
            logger.warn("This impl register metrics for only EntryEventSink, skip.");
        }
    }

//    private void initStoreCollector(CanalInstance instance) {
//        try {
//            storeCollector = new StoreCollector(instance.getEventStore(), destination);
//        } catch (CanalStoreException cse) {
//            logger.warn("Failed to register metrics for destination {}.", destination, cse);
//        }
//    }

    private void initMetaCollector(CanalInstance instance) {
        metaCollector = new MetaCollector(instance);
    }

    private void initSinkCollector(CanalInstance instance) {
//        sinkCollector = new EntryCollector(instance.getEventSink(), instance.getDestination());
    }

    private void initParserCollector(CanalInstance instance) {
        parserCollector = new MysqlParserCollector(instance.getEventParser(), instance.getDestination());
    }
}
