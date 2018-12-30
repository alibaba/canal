package com.alibaba.otter.canal.deployer;

import java.io.FileInputStream;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.deployer.monitor.ManagerRemoteConfigMonitor;

/**
 * canal独立版本启动的入口类
 *
 * @author jianghang 2012-11-6 下午05:20:49
 * @version 1.0.0
 */
public class CanalLauncher {

    private static final String    CLASSPATH_URL_PREFIX = "classpath:";
    private static final Logger    logger               = LoggerFactory.getLogger(CanalLauncher.class);
    public static volatile boolean running              = false;

    public static void main(String[] args) {
        try {
            running = true;
            logger.info("## set default uncaught exception handler");
            setGlobalUncaughtExceptionHandler();

            logger.info("## load canal configurations");
            String conf = System.getProperty("canal.conf", "classpath:canal.properties");
            Properties properties = new Properties();
            ManagerRemoteConfigMonitor managerDbConfigMonitor = null;
            if (conf.startsWith(CLASSPATH_URL_PREFIX)) {
                conf = StringUtils.substringAfter(conf, CLASSPATH_URL_PREFIX);
                properties.load(CanalLauncher.class.getClassLoader().getResourceAsStream(conf));
            } else {
                properties.load(new FileInputStream(conf));
            }

            String jdbcUrl = properties.getProperty("canal.manager.jdbc.url");
            if (!StringUtils.isEmpty(jdbcUrl)) {
                logger.info("## load remote canal configurations");
                // load remote config
                String jdbcUsername = properties.getProperty("canal.manager.jdbc.username");
                String jdbcPassword = properties.getProperty("canal.manager.jdbc.password");
                managerDbConfigMonitor = new ManagerRemoteConfigMonitor(jdbcUrl, jdbcUsername, jdbcPassword);
                // 加载远程canal.properties
                Properties remoteConfig = managerDbConfigMonitor.loadRemoteConfig();
                // 加载remote instance配置
                managerDbConfigMonitor.loadRemoteInstanceConfigs();
                if (remoteConfig != null) {
                    properties = remoteConfig;
                } else {
                    managerDbConfigMonitor = null;
                }
            } else {
                logger.info("## load canal configurations");
            }

            final CanalStater canalStater = new CanalStater();
            canalStater.start(properties);

            if (managerDbConfigMonitor != null) {
                managerDbConfigMonitor.start(new ManagerRemoteConfigMonitor.Listener<Properties>() {

                    @Override
                    public void onChange(Properties properties) {
                        try {
                            // 远程配置canal.properties修改重新加载整个应用
                            canalStater.destroy();
                            canalStater.start(properties);
                        } catch (Throwable throwable) {
                            logger.error(throwable.getMessage(), throwable);
                        }
                    }
                });
            }

            while (running)
                ;

            if (managerDbConfigMonitor != null) {
                managerDbConfigMonitor.destroy();
            }
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
