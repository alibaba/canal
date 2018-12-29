package com.alibaba.otter.canal.deployer.monitor;

import java.io.ByteArrayInputStream;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.MapMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.common.utils.NamedThreadFactory;
import com.alibaba.otter.canal.deployer.CanalConstants;
import com.google.common.base.Function;
import com.google.common.collect.MigrateMap;

public class ManagerDbConfigMonitor {

    private static final Logger      logger                 = LoggerFactory.getLogger(ManagerDbConfigMonitor.class);

    private Map<String, ConfigItem>  remoteInstanceConfigs  = new MapMaker().makeMap();

    private Connection               conn;
    private String                   jdbcUrl;
    private String                   jdbcUsername;
    private String                   jdbcPassword;

    private long                     currentConfigTimestamp = 0;

    private long                     scanIntervalInSecond   = 5;
    private ScheduledExecutorService executor               = Executors.newScheduledThreadPool(1,
        new NamedThreadFactory("remote-canal-config-scan"));

    public ManagerDbConfigMonitor(String jdbcUrl, String jdbcUsername, String jdbcPassword){
        this.jdbcUrl = jdbcUrl;
        this.jdbcUsername = jdbcUsername;
        this.jdbcPassword = jdbcPassword;
    }

    public void setScanIntervalInSecond(long scanIntervalInSecond) {
        this.scanIntervalInSecond = scanIntervalInSecond;
    }

    private Connection getConn() throws Exception {
        if (conn == null || conn.isClosed()) {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
        }
        return conn;
    }

    public Properties loadRemoteConfig() {
        Properties properties = null;
        try {
            ConfigItem configItem = getRemoteCanalConfig();
            if (configItem != null) {
                if (configItem.getModifiedTime() != currentConfigTimestamp) {
                    currentConfigTimestamp = configItem.getModifiedTime();
                    overrideLocalCanalConfig(configItem.getContent());
                    properties = new Properties();
                    properties.load(new ByteArrayInputStream(configItem.getContent().getBytes(StandardCharsets.UTF_8)));
                    scanIntervalInSecond = Integer
                        .valueOf(properties.getProperty(CanalConstants.CANAL_AUTO_SCAN_INTERVAL, "5"));
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return properties;
    }

    private ConfigItem getRemoteCanalConfig() {
        String sql = "select name, content, modified_time from canal_config where id=1";
        try (Statement stmt = getConn().createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
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

    private void overrideLocalCanalConfig(String content) {
        try (FileWriter writer = new FileWriter("../conf/canal.properties")) {
            writer.write(content);
            writer.flush();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void start(final Listener<Properties> listener) {
        executor.scheduleWithFixedDelay(new Runnable() {

            public void run() {
                try {
                    Properties properties = loadRemoteConfig();
                    if (properties != null) {
                        // 重启整个进程
                        listener.onChange(properties);
                    }
                } catch (Throwable e) {
                    logger.error("scan failed", e);
                }
            }

        }, 10, scanIntervalInSecond, TimeUnit.SECONDS);
    }

    public interface Listener<Properties> {

        void onChange(Properties properties);
    }

    private Map<String, ConfigItem> getChangedInstanceConfigs() {
        String sql = "select name, content, modified_time from instance_config";
        try (Statement stmt = getConn().createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            Map<String, ConfigItem> changedInstanceConfig = new HashMap<>();
            while (rs.next()) {
                ConfigItem configItemNew = new ConfigItem();
                configItemNew.setName(rs.getString("name"));
                configItemNew.setContent(rs.getString("content"));
                configItemNew.setModifiedTime(rs.getTimestamp("modified_time").getTime());

                ConfigItem configItem = remoteInstanceConfigs.get(configItemNew.getName());
                if (configItem == null) {
                    remoteInstanceConfigs.put(configItemNew.getName(), configItemNew);
                    changedInstanceConfig.put(configItemNew.getName(), configItemNew);
                } else {
                    if (configItem.getModifiedTime() != configItemNew.getModifiedTime()) {
                        remoteInstanceConfigs.put(configItemNew.getName(), configItemNew);
                        changedInstanceConfig.put(configItemNew.getName(), configItemNew);
                    }
                }
            }
            return changedInstanceConfig.isEmpty() ? null : changedInstanceConfig;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    private void overrideLocalInstanceConfigs(Map<String, ConfigItem> changedInstanceConfigs) {
        if (changedInstanceConfigs != null) {
            for (ConfigItem configItem : changedInstanceConfigs.values()) {
                try (FileWriter writer = new FileWriter("../conf/" + configItem.getName() + "/instance.properties")) {
                    writer.write(configItem.getContent());
                    writer.flush();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

    public void destroy() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    public static class ConfigItem {

        private Long   id;
        private String name;
        private String content;
        private long   modifiedTime;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getContent() {
            return content;
        }

        public void setContent(String content) {
            this.content = content;
        }

        public long getModifiedTime() {
            return modifiedTime;
        }

        public void setModifiedTime(long modifiedTime) {
            this.modifiedTime = modifiedTime;
        }
    }

}
