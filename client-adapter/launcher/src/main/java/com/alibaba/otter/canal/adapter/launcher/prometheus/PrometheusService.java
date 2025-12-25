package com.alibaba.otter.canal.adapter.launcher.prometheus;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;

/**
 * @author sunxien
 * @date 2025/5/19
 * @since 1.0.0-SNAPSHOT
 */
@Component
public class PrometheusService implements InitializingBean, DisposableBean {

    private static final Logger logger = LoggerFactory.getLogger(PrometheusService.class);

    private final CanalAdapterExports adapterExports;
    private volatile boolean running = false;

    /**
     * <pre>
     * Canal Server admin port: 11110
     * Canal Server tcp port: 11111
     * Canal Server metric port: 11112
     * Canal Adapter metric port: 11113
     * </pre>
     */
    private int port = 11113;
    private HTTPServer httpServer;

    private PrometheusService() {
        this.adapterExports = CanalAdapterExports.instance();
    }

    private static class SingletonHolder {
        private static final PrometheusService SINGLETON = new PrometheusService();
    }

    public static PrometheusService getInstance() {
        return SingletonHolder.SINGLETON;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        PrometheusService.getInstance().initialize();
    }

    @Override
    public void destroy() throws Exception {
        PrometheusService.getInstance().terminate();
    }

    public void initialize() {
        try {
            logger.info("Starting prometheus HTTPServer on port {}....", port);
            // TODO 2.Https?
            httpServer = new HTTPServer(port);
            logger.info("Start prometheus HTTPServer on port {} success", port);
        } catch (IOException e) {
            logger.error("Unable to start prometheus HTTPServer on port {}.", port, e);
            return;
        }
        try {
            // JVM exports
            DefaultExports.initialize();
            // adapterExports.initialize();
            this.running = true;
        } catch (Throwable t) {
            logger.error("Unable to initialize adapter exports. (Register the default Hotspot collectors)", t);
        }
    }

    public void terminate() {
        try {
            this.running = false;
            adapterExports.terminate();
            if (httpServer != null) {
                httpServer.stop();
                logger.info("Stop prometheus HTTPServer on port {} success", port);
            }
        } catch (Throwable t) {
            logger.error("Something happened while terminating prometheus HTTPServer.", t);
        }
    }

    public boolean isRunning() {
        return running;
    }

    public void setServerPort(int port) {
        this.port = port;
    }
}
