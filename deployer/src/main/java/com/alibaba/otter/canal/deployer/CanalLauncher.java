package com.alibaba.otter.canal.deployer;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.common.utils.NamedThreadFactory;
import com.alibaba.otter.canal.instance.manager.plain.PlainCanal;
import com.alibaba.otter.canal.instance.manager.plain.PlainCanalConfigClient;

/**
 * canal独立版本启动的入口类
 *
 * @author jianghang 2012-11-6 下午05:20:49
 * @version 1.0.0
 */
public class CanalLauncher {

    private static final String             CLASSPATH_URL_PREFIX = "classpath:";
    private static final Logger             logger               = LoggerFactory.getLogger(CanalLauncher.class);
    public static final CountDownLatch      runningLatch         = new CountDownLatch(1);
    private static ScheduledExecutorService executor             = Executors.newScheduledThreadPool(1,
        new NamedThreadFactory("canal-server-scan"));

    public static void main(String[] args) {
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

            final CanalStater canalStater = new CanalStater(properties);
            String managerAddress = properties.getProperty(CanalConstants.CANAL_ADMIN_MANAGER);
            if (StringUtils.isNotEmpty(managerAddress)) {
                String user = properties.getProperty(CanalConstants.CANAL_ADMIN_USER);
                String passwd = properties.getProperty(CanalConstants.CANAL_ADMIN_PASSWD);
                String adminPort = properties.getProperty(CanalConstants.CANAL_ADMIN_PORT, "11110");
                final PlainCanalConfigClient configClient = new PlainCanalConfigClient(managerAddress,
                    user,
                    passwd,
                    "",
                    Integer.parseInt(adminPort));
                PlainCanal canalConfig = configClient.findServer(null);
                if (canalConfig != null) {
                    properties = canalConfig.getProperties();
                    // 用本地配置覆盖
                    properties.put(CanalConstants.CANAL_ADMIN_MANAGER, managerAddress);
                    properties.put(CanalConstants.CANAL_ADMIN_USER, user);
                    properties.put(CanalConstants.CANAL_ADMIN_PASSWD, passwd);
                    properties.put(CanalConstants.CANAL_ADMIN_PORT, adminPort);
                    int scanIntervalInSecond = Integer
                        .parseInt(properties.getProperty(CanalConstants.CANAL_AUTO_SCAN_INTERVAL, "5"));
                    executor.scheduleWithFixedDelay(new Runnable() {

                        private PlainCanal lastCanalConfig;

                        public void run() {
                            try {
                                if (lastCanalConfig == null) {
                                    lastCanalConfig = configClient.findServer(null);
                                } else {
                                    PlainCanal newCanalConfig = configClient.findServer(lastCanalConfig.getMd5());
                                    if (newCanalConfig != null) {
                                        // 远程配置canal.properties修改重新加载整个应用
                                        canalStater.stop();
                                        Properties properties1 = newCanalConfig.getProperties();
                                        // 用本地配置覆盖
                                        properties1.put(CanalConstants.CANAL_ADMIN_MANAGER, managerAddress);
                                        properties1.put(CanalConstants.CANAL_ADMIN_USER, user);
                                        properties1.put(CanalConstants.CANAL_ADMIN_PASSWD, passwd);
                                        properties1.put(CanalConstants.CANAL_ADMIN_PORT, adminPort);
                                        canalStater.setProperties(properties1);
                                        canalStater.start();

                                        lastCanalConfig = newCanalConfig;
                                    }
                                }

                            } catch (Throwable e) {
                                logger.error("scan failed", e);
                            }
                        }

                    }, 0, scanIntervalInSecond, TimeUnit.SECONDS);
                }
            }

            canalStater.setProperties(properties);
            canalStater.start();
            runningLatch.await();
            executor.shutdownNow();
        } catch (Throwable e) {
            logger.error("## Something goes wrong when starting up the canal Server:", e);
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
