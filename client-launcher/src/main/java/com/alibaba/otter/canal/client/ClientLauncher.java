package com.alibaba.otter.canal.client;

import java.io.FileInputStream;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.alibaba.otter.canal.client.adapter.loader.CanalAdapterLoader;
import com.alibaba.otter.canal.client.adapter.support.CanalClientConfig;

public class ClientLauncher {

    private static final String CLASSPATH_URL_PREFIX = "classpath:";
    private static final Logger logger               = LoggerFactory.getLogger(ClientLauncher.class);

    public static void main(String[] args) {
        try {
            logger.info("## set default uncaught exception handler");
            setGlobalUncaughtExceptionHandler();

            logger.info("## load canal client configurations");
            String conf = System.getProperty("client.conf", "classpath:canal-client.yml");
            CanalClientConfig canalClientConfig;
            if (conf.startsWith(CLASSPATH_URL_PREFIX)) {
                conf = StringUtils.substringAfter(conf, CLASSPATH_URL_PREFIX);
                canalClientConfig = new Yaml().loadAs(ClientLauncher.class.getClassLoader().getResourceAsStream(conf),
                    CanalClientConfig.class);
            } else {
                canalClientConfig = new Yaml().loadAs(new FileInputStream(conf), CanalClientConfig.class);
            }
            logger.info("## start the canal client adapters.");
            final CanalAdapterLoader adapterLoader = new CanalAdapterLoader(canalClientConfig);
            adapterLoader.init();
            logger.info("## the canal client adapters are running now ......");
            Runtime.getRuntime().addShutdownHook(new Thread() {

                public void run() {
                    try {
                        logger.info("## stop the canal client adapters");
                        adapterLoader.destroy();
                    } catch (Throwable e) {
                        logger.warn("## something goes wrong when stopping canal client adapters:", e);
                    } finally {
                        logger.info("## canal client adapters are down.");
                    }
                }

            });
        } catch (Throwable e) {
            logger.error("## something goes wrong when starting up the canal client adapters:", e);
            System.exit(0);
        }
    }

    private static void setGlobalUncaughtExceptionHandler() {
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {

            @Override
            public void uncaughtException(Thread t, Throwable e) {
                logger.error("UnCaughtException", e);
            }
        });
    }
}
