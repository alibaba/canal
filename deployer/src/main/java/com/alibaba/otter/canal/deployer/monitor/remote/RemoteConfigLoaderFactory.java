package com.alibaba.otter.canal.deployer.monitor.remote;

import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 远程配置装载器工厂类
 *
 * @author rewerma 2019-01-25 下午05:20:16
 * @version 1.0.0
 */
public class RemoteConfigLoaderFactory {

    private static final Logger logger = LoggerFactory.getLogger(RemoteConfigLoaderFactory.class);

    public static RemoteConfigLoader getRemoteConfigLoader(Properties localProperties) {
        String jdbcUrl = localProperties.getProperty("canal.manager.jdbc.url");
        if (!StringUtils.isEmpty(jdbcUrl)) {
            logger.info("## load remote canal configurations");
            // load remote config
            String driverName = localProperties.getProperty("canal.manager.jdbc.driverName");
            String jdbcUsername = localProperties.getProperty("canal.manager.jdbc.username");
            String jdbcPassword = localProperties.getProperty("canal.manager.jdbc.password");
            return new DbRemoteConfigLoader(driverName, jdbcUrl, jdbcUsername, jdbcPassword);
        }
        // 可扩展其它远程配置加载器

        logger.info("## load local canal configurations");

        return null;
    }
}
