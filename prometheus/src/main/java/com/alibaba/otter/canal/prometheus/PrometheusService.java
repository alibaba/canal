package com.alibaba.otter.canal.prometheus;

import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.prometheus.impl.PrometheusClientInstanceProfiler;
import com.alibaba.otter.canal.server.netty.ClientInstanceProfiler;
import com.alibaba.otter.canal.spi.CanalMetricsService;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.alibaba.otter.canal.server.netty.CanalServerWithNettyProfiler.NOP;
import static com.alibaba.otter.canal.server.netty.CanalServerWithNettyProfiler.profiler;

/**
 * @author Chuanyi Li
 */
public class PrometheusService implements CanalMetricsService {

    private static final Logger          logger          = LoggerFactory.getLogger(PrometheusService.class);
    private final CanalInstanceExports   instanceExports;
    private volatile boolean             running         = false;
    private int                          port;
    private HTTPServer                   server;
    private final ClientInstanceProfiler clientProfiler;

    private PrometheusService() {
        this.instanceExports = CanalInstanceExports.instance();
        this.clientProfiler = PrometheusClientInstanceProfiler.instance();
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
            logger.info("Start prometheus HTTPServer on port {}.", port);
            //TODO 2.Https?
            server = new HTTPServer(port);
        } catch (IOException e) {
            logger.warn("Unable to start prometheus HTTPServer.", e);
            return;
        }
        try {
            // JVM exports
            DefaultExports.initialize();
            instanceExports.initialize();
            if (!clientProfiler.isStart()) {
                clientProfiler.start();
            }
            profiler().setInstanceProfiler(clientProfiler);
        } catch (Throwable t) {
            logger.warn("Unable to initialize server exports.", t);
        }

        running = true;
    }

    @Override
    public void terminate() {
        running = false;
        try {
            instanceExports.terminate();
            if (clientProfiler.isStart()) {
                clientProfiler.stop();
            }
            profiler().setInstanceProfiler(NOP);
            if (server != null) {
                server.stop();
            }
        } catch (Throwable t) {
            logger.warn("Something happened while terminating.", t);
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
            instanceExports.register(instance);
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
            instanceExports.unregister(instance);
        } catch (Throwable t) {
            logger.warn("Unable to unregister instance exports for {}.", instance.getDestination(), t);
        }
        logger.info("Unregister metrics for destination {}.", instance.getDestination());
    }

    @Override
    public void setServerPort(int port) {
        this.port = port;
    }

}
