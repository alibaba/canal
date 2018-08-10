package com.alibaba.otter.canal.prometheus;

import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.prometheus.impl.*;
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

    private static final Logger      logger           = LoggerFactory.getLogger(CanalInstanceExports.class);
    public static final String       DEST             = "destination";
    public static final String[]     DEST_LABELS      = {DEST};
    public static final List<String> DEST_LABELS_LIST = Collections.singletonList(DEST);
    private final Collector          storeCollector;
    private final Collector          entryCollector;
    private final Collector          metaCollector;
    private final Collector          sinkCollector;
    private final Collector          parserCollector;

    private CanalInstanceExports() {
        this.storeCollector = StoreCollector.instance();
        this.entryCollector = EntryCollector.instance();
        this.metaCollector = MetaCollector.instance();
        this.sinkCollector = SinkCollector.instance();
        this.parserCollector = ParserCollector.instance();
    }

    private static class SingletonHolder {
        private static final CanalInstanceExports SINGLETON = new CanalInstanceExports();
    }

    public static CanalInstanceExports instance() {
        return SingletonHolder.SINGLETON;
    }

    public void initialize() {
        storeCollector.register();
        entryCollector.register();
        metaCollector.register();
        sinkCollector.register();
        parserCollector.register();
    }

    public void terminate() {
        CollectorRegistry.defaultRegistry.unregister(storeCollector);
        CollectorRegistry.defaultRegistry.unregister(entryCollector);
        CollectorRegistry.defaultRegistry.unregister(metaCollector);
        CollectorRegistry.defaultRegistry.unregister(sinkCollector);
        CollectorRegistry.defaultRegistry.unregister(parserCollector);
    }

    void register(CanalInstance instance) {
        requiredInstanceRegistry(storeCollector).register(instance);
        requiredInstanceRegistry(entryCollector).register(instance);
        requiredInstanceRegistry(metaCollector).register(instance);
        requiredInstanceRegistry(sinkCollector).register(instance);
        requiredInstanceRegistry(parserCollector).register(instance);
        logger.info("Successfully register metrics for instance {}.", instance.getDestination());
    }

    void unregister(CanalInstance instance) {
        requiredInstanceRegistry(storeCollector).unregister(instance);
        requiredInstanceRegistry(entryCollector).unregister(instance);
        requiredInstanceRegistry(metaCollector).unregister(instance);
        requiredInstanceRegistry(sinkCollector).unregister(instance);
        requiredInstanceRegistry(parserCollector).unregister(instance);
        logger.info("Successfully unregister metrics for instance {}.", instance.getDestination());
    }

    private InstanceRegistry requiredInstanceRegistry(Collector collector) {
        if (!(collector instanceof InstanceRegistry)) {
            throw new IllegalArgumentException("Canal prometheus collector need to implement InstanceRegistry.");
        }
        return (InstanceRegistry) collector;
    }

}
