package com.alibaba.otter.canal.deployer.monitor.remote;

import java.io.ByteArrayInputStream;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.common.utils.CommonUtils;
import com.alibaba.otter.canal.common.utils.NamedThreadFactory;
import com.alibaba.otter.canal.deployer.CanalConstants;
import com.google.common.base.Joiner;
import com.google.common.collect.MapMaker;

/**
 * 基于数据库的远程配置装载器
 *
 * @author rewerma 2019-01-25 下午05:20:16
 * @version 1.0.0
 */
public class DbRemoteConfigLoader implements RemoteConfigLoader {

    private static final Logger      logger                 = LoggerFactory.getLogger(DbRemoteConfigLoader.class);

    private Map<String, ConfigItem>  remoteInstanceConfigs  = new MapMaker().makeMap();

    private DruidDataSource          dataSource;

    private long                     currentConfigTimestamp = 0;

    private long                     scanIntervalInSecond   = 5;
    private ScheduledExecutorService executor               = Executors.newScheduledThreadPool(2,
        new NamedThreadFactory("remote-canal-config-scan"));

    private RemoteInstanceMonitor    remoteInstanceMonitor  = new RemoteInstanceMonitorImpl();

    public DbRemoteConfigLoader(String driverName, String jdbcUrl, String jdbcUsername, String jdbcPassword){
        dataSource = new DruidDataSource();
        if (StringUtils.isEmpty(driverName)) {
            driverName = "com.mysql.jdbc.Driver";
        }
        dataSource.setDriverClassName(driverName);
        dataSource.setUrl(jdbcUrl);
        dataSource.setUsername(jdbcUsername);
        dataSource.setPassword(jdbcPassword);
        dataSource.setInitialSize(1);
        dataSource.setMinIdle(1);
        dataSource.setMaxActive(1);
        dataSource.setMaxWait(60000);
        dataSource.setTimeBetweenEvictionRunsMillis(60000);
        dataSource.setMinEvictableIdleTimeMillis(300000);
        try {
            dataSource.init();
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * 加载远程 canal.properties文件
     *
     * @return 远程配置的properties
     */
    @Override
    public Properties loadRemoteConfig() {
        Properties properties = null;
        try {
            // 加载远程canal配置
            ConfigItem configItem = getRemoteCanalConfig();
            if (configItem != null) {
                if (configItem.getModifiedTime() != currentConfigTimestamp) {
                    currentConfigTimestamp = configItem.getModifiedTime();
                    overrideLocalCanalConfig(configItem.getContent());
                    properties = new Properties();
                    properties.load(new ByteArrayInputStream(configItem.getContent().getBytes(StandardCharsets.UTF_8)));
                    scanIntervalInSecond = Integer
                        .valueOf(properties.getProperty(CanalConstants.CANAL_AUTO_SCAN_INTERVAL, "5"));
                    logger.info("## Loaded remote canal config: canal.properties ");
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return properties;
    }

    /**
     * 覆盖本地 canal.properties
     *
     * @param content 远程配置内容文本
     */
    private void overrideLocalCanalConfig(String content) {
        try (FileWriter writer = new FileWriter(CommonUtils.getConfPath() + "canal.properties")) {
            writer.write(content);
            writer.flush();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 获取远程canal.properties配置内容
     *
     * @return 内容对象
     */
    private ConfigItem getRemoteCanalConfig() {
        String sql = "select name, content, modified_time from canal_config where id=1";
        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                ConfigItem configItem = new ConfigItem();
                configItem.setId(1L);
                configItem.setName(rs.getString("name"));
                configItem.setContent(rs.getString("content"));
                configItem.setModifiedTime(rs.getTimestamp("modified_time").getTime());
                return configItem;
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    /**
     * 加载远程的instance配置
     */
    @Override
    public void loadRemoteInstanceConfigs() {
        try {
            // 加载远程instance配置
            loadModifiedInstanceConfigs();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 加载远程instance新增、修改、删除配置
     */
    private void loadModifiedInstanceConfigs() {
        Map<String, ConfigItem> remoteConfigStatus = new HashMap<>();
        String sql = "select id, name, modified_time from canal_instance_config";
        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                ConfigItem configItem = new ConfigItem();
                configItem.setId(rs.getLong("id"));
                configItem.setName(rs.getString("name"));
                configItem.setModifiedTime(rs.getTimestamp("modified_time").getTime());
                remoteConfigStatus.put(configItem.getName(), configItem);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        if (!remoteConfigStatus.isEmpty()) {
            List<Long> changedIds = new ArrayList<>();

            for (ConfigItem remoteConfigStat : remoteConfigStatus.values()) {
                ConfigItem currentConfig = remoteInstanceConfigs.get(remoteConfigStat.getName());
                if (currentConfig == null) {
                    // 新增
                    changedIds.add(remoteConfigStat.getId());
                } else {
                    // 修改
                    if (currentConfig.getModifiedTime() != remoteConfigStat.getModifiedTime()) {
                        changedIds.add(remoteConfigStat.getId());
                    }
                }
            }
            if (!changedIds.isEmpty()) {
                String contentsSql = "select id, name, content, modified_time from canal_instance_config  where id in ("
                                     + Joiner.on(",").join(changedIds) + ")";
                try (Connection conn = dataSource.getConnection();
                        Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery(contentsSql)) {
                    while (rs.next()) {
                        ConfigItem configItemNew = new ConfigItem();
                        configItemNew.setId(rs.getLong("id"));
                        configItemNew.setName(rs.getString("name"));
                        configItemNew.setContent(rs.getString("content"));
                        configItemNew.setModifiedTime(rs.getTimestamp("modified_time").getTime());

                        remoteInstanceConfigs.put(configItemNew.getName(), configItemNew);
                        remoteInstanceMonitor.onModify(configItemNew);
                    }

                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }

        for (String name : remoteInstanceConfigs.keySet()) {
            if (!remoteConfigStatus.containsKey(name)) {
                // 删除
                remoteInstanceConfigs.remove(name);
                remoteInstanceMonitor.onDelete(name);
            }
        }
    }

    /**
     * 监听 canal 主配置和 instance 配置变化
     *
     * @param remoteCanalConfigMonitor 监听回调方法
     */
    public void startMonitor(final RemoteCanalConfigMonitor remoteCanalConfigMonitor) {
        // 监听canal.properties变化
        executor.scheduleWithFixedDelay(new Runnable() {

            public void run() {
                try {
                    Properties properties = loadRemoteConfig();
                    if (properties != null) {
                        remoteCanalConfigMonitor.onChange(properties);
                    }
                } catch (Throwable e) {
                    logger.error("Scan remote canal config failed", e);
                }
            }

        }, 10, scanIntervalInSecond, TimeUnit.SECONDS);

        // 监听instance变化
        executor.scheduleWithFixedDelay(new Runnable() {

            public void run() {
                try {
                    loadRemoteInstanceConfigs();
                } catch (Throwable e) {
                    logger.error("Scan remote instance config failed", e);
                }
            }

        }, 10, 3, TimeUnit.SECONDS);
    }

    /**
     * 销毁
     */
    public void destroy() {
        executor.shutdownNow();
        try {
            dataSource.close();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

}
