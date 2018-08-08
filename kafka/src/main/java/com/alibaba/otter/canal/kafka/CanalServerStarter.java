package com.alibaba.otter.canal.kafka;

import java.io.FileInputStream;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.deployer.CanalController;

/**
 * canal server 启动类
 *
 * @author machengyuan 2018-6-11 下午05:30:49
 * @version 1.0.0
 */
public class CanalServerStarter {

    private static final String     CLASSPATH_URL_PREFIX = "classpath:";
    private static final Logger     logger               = LoggerFactory.getLogger(CanalServerStarter.class);
    private volatile static boolean running              = false;

    public static void init() {
        try {
            logger.info("## set default uncaught exception handler");
            setGlobalUncaughtExceptionHandler();

            logger.info("## load canal configurations");
            String conf = System.getProperty("canal.conf", "classpath:canal.properties");
            Properties properties = new Properties();
            if (conf.startsWith(CLASSPATH_URL_PREFIX)) {
                conf = StringUtils.substringAfter(conf, CLASSPATH_URL_PREFIX);
                properties.load(CanalLauncher.class.getClassLoader().getResourceAsStream(conf));
            } else {
                properties.load(new FileInputStream(conf));
            }

            logger.info("## start the canal server.");
            final CanalController controller = new CanalController(properties);
            controller.start();
            running = true;
            logger.info("## the canal server is running now ......");
            Runtime.getRuntime().addShutdownHook(new Thread() {

                public void run() {
                    try {
                        logger.info("## stop the canal server");
                        running = false;
                        controller.stop();
                    } catch (Throwable e) {
                        logger.warn("##something goes wrong when stopping canal Server:", e);
                    } finally {
                        logger.info("## canal server is down.");
                    }
                }

            });
        } catch (Throwable e) {
            logger.error("## Something goes wrong when starting up the canal Server:", e);
            System.exit(0);
        }
    }

    public static boolean isRunning() {
        return running;
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
