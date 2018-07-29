package com.alibaba.otter.canal.prometheus;

import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.spi.CanalMetricsService;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Chuanyi Li
 */
public class PrometheusService implements CanalMetricsService {

    private static final Logger                           logger  = LoggerFactory.getLogger(PrometheusService.class);

    private final Map<String, CanalInstanceExports>       exports = new ConcurrentHashMap<String, CanalInstanceExports>();

    private volatile boolean                              running = false;

    private HTTPServer                                    server;

    private PrometheusService() {
    }

    private static class SingletonHolder {
        private static final PrometheusService SINGLETON = new PrometheusService();
    }

    public static PrometheusService getInstance() {
        return SingletonHolder.SINGLETON;
    }

    @Override
    public void initialize() {
        try {
            //TODO 1.Configurable port
            //TODO 2.Https
            server = new HTTPServer(11112);
        } catch (IOException e) {
            logger.warn("Unable to start prometheus HTTPServer.", e);
            return;
        }
        try {
            // JVM exports
            DefaultExports.initialize();
            // Canal server level exports
            CanalServerExports.initialize();
        } catch (Throwable t) {
            logger.warn("Unable to initialize server exports.", t);
        }

        running = true;
    }

    @Override
    public void terminate() {
        running = false;
        // Normally, service should be terminated at canal shutdown.
        // No need to unregister instance exports explicitly.
        // But for the sake of safety, unregister them.
        for (CanalInstanceExports ie : exports.values()) {
            ie.unregister();
        }
        if (server != null) {
            server.stop();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public void register(CanalInstance instance) {
        if (instance.isStart()) {
            logger.warn("Cannot register metrics for destination {} that is running.", instance.getDestination());
            return;
        }
        try {
            CanalInstanceExports export = CanalInstanceExports.forInstance(instance);
            export.register();
            exports.put(instance.getDestination(), export);
        } catch (Throwable t) {
            logger.warn("Unable to register instance exports for {}.", instance.getDestination(), t);
        }
        logger.info("Register metrics for destination {}.", instance.getDestination());
    }

    @Override
    public void unregister(CanalInstance instance) {
        if (instance.isStart()) {
            logger.warn("Try unregister metrics after destination {} is stopped.", instance.getDestination());
        }
        try {
            CanalInstanceExports export = exports.remove(instance.getDestination());
            if (export != null) {
                export.unregister();
            }
        } catch (Throwable t) {
            logger.warn("Unable to unregister instance exports for {}.", instance.getDestination(), t);
        }
        logger.info("Unregister metrics for destination {}.", instance.getDestination());
    }
}
