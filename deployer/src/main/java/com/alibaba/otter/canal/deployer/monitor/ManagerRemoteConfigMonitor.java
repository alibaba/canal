package com.alibaba.otter.canal.deployer.monitor;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.common.utils.NamedThreadFactory;
import com.alibaba.otter.canal.deployer.CanalConstants;
import com.google.common.base.Joiner;
import com.google.common.collect.MapMaker;

/**
 * 远程配置装载监控
 * 
 * @author rewerma 2018-12-30 下午05:12:16
 * @version 1.0.1
 */
public class ManagerRemoteConfigMonitor {

    private static final Logger      logger                 = LoggerFactory.getLogger(ManagerRemoteConfigMonitor.class);

    private Map<String, ConfigItem>  remoteInstanceConfigs  = new MapMaker().makeMap();

    private Connection               conn;
    private String                   jdbcUrl;
    private String                   jdbcUsername;
    private String                   jdbcPassword;

    private long                     currentConfigTimestamp = 0;

    private long                     scanIntervalInSecond   = 5;
    private ScheduledExecutorService executor               = Executors.newScheduledThreadPool(2,
        new NamedThreadFactory("remote-canal-config-scan"));

    public ManagerRemoteConfigMonitor(String jdbcUrl, String jdbcUsername, String jdbcPassword){
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

    /**
     * 加载远程 canal.properties文件 覆盖本地
     * 
     * @return 远程配置的properties
     */
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
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return properties;
    }

    /**
     * 加载远程的instance配置 覆盖本地
     */
    public void loadRemoteInstanceConfigs() {
        try {
            // 加载远程instance配置
            Map<String, ConfigItem>[] modifiedConfigs = getModifiedInstanceConfigs();
            if (modifiedConfigs != null) {
                overrideLocalInstanceConfigs(modifiedConfigs);
            }
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

    /**
     * 覆盖本地 canal.properties
     * 
     * @param content 远程配置内容文本
     */
    private void overrideLocalCanalConfig(String content) {
        try (FileWriter writer = new FileWriter(getConfPath() + "canal.properties")) {
            writer.write(content);
            writer.flush();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 获取远程instance新增、修改、删除配置
     *
     * @return Map[0]：新增或修改的instance配置; Map[1]：删除的instance配置
     */
    @SuppressWarnings("unchecked")
    private Map<String, ConfigItem>[] getModifiedInstanceConfigs() {
        Map<String, ConfigItem>[] res = new Map[2];
        Map<String, ConfigItem> remoteConfigStatus = new HashMap<>();
        String sql = "select id, name, modified_time from canal_instance_config";
        try (Statement stmt = getConn().createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                ConfigItem configItem = new ConfigItem();
                configItem.setId(rs.getLong("id"));
                configItem.setName(rs.getString("name"));
                configItem.setModifiedTime(rs.getTimestamp("modified_time").getTime());
                remoteConfigStatus.put(configItem.getName(), configItem);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return null;
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
                Map<String, ConfigItem> changedInstanceConfig = new HashMap<>();
                String contentsSql = "select id, name, content, modified_time from canal_instance_config  where id in ("
                                     + Joiner.on(",").join(changedIds) + ")";
                try (Statement stmt = getConn().createStatement(); ResultSet rs = stmt.executeQuery(contentsSql)) {
                    while (rs.next()) {
                        ConfigItem configItemNew = new ConfigItem();
                        configItemNew.setId(rs.getLong("id"));
                        configItemNew.setName(rs.getString("name"));
                        configItemNew.setContent(rs.getString("content"));
                        configItemNew.setModifiedTime(rs.getTimestamp("modified_time").getTime());

                        remoteInstanceConfigs.put(configItemNew.getName(), configItemNew);
                        changedInstanceConfig.put(configItemNew.getName(), configItemNew);
                    }

                    res[0] = changedInstanceConfig.isEmpty() ? null : changedInstanceConfig;
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }

        Map<String, ConfigItem> removedInstanceConfig = new HashMap<>();
        for (String name : remoteInstanceConfigs.keySet()) {
            if (!remoteConfigStatus.containsKey(name)) {
                // 删除
                remoteInstanceConfigs.remove(name);
                removedInstanceConfig.put(name, null);
            }
        }
        res[1] = removedInstanceConfig.isEmpty() ? null : removedInstanceConfig;

        if (res[0] == null && res[1] == null) {
            return null;
        } else {
            return res;
        }
    }

    /**
     * 覆盖本地instance配置
     * 
     * @param modifiedInstanceConfigs 有变更的配置项
     */
    private void overrideLocalInstanceConfigs(Map<String, ConfigItem>[] modifiedInstanceConfigs) {
        Map<String, ConfigItem> changedInstanceConfigs = modifiedInstanceConfigs[0];
        if (changedInstanceConfigs != null) {
            for (ConfigItem configItem : changedInstanceConfigs.values()) {
                try (FileWriter writer = new FileWriter(
                    getConfPath() + configItem.getName() + "/instance.properties")) {
                    writer.write(configItem.getContent());
                    writer.flush();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
        Map<String, ConfigItem> removedInstanceConfigs = modifiedInstanceConfigs[1];
        if (removedInstanceConfigs != null) {
            for (String name : removedInstanceConfigs.keySet()) {
                File file = new File(getConfPath() + name + "/");
                if (file.exists()) {
                    file.delete();
                }
            }
        }
    }

    /**
     * 监听 canal 主配置和 instance 配置变化
     * 
     * @param listener 监听回调方法
     */
    public void start(final Listener<Properties> listener) {
        // 监听canal.properties变化
        executor.scheduleWithFixedDelay(new Runnable() {

            public void run() {
                try {
                    Properties properties = loadRemoteConfig();
                    if (properties != null) {
                        listener.onChange(properties);
                    }
                } catch (Throwable e) {
                    logger.error("scan failed", e);
                }
            }

        }, 10, scanIntervalInSecond, TimeUnit.SECONDS);

        // 监听instance变化
        executor.scheduleWithFixedDelay(new Runnable() {

            public void run() {
                try {
                    loadRemoteInstanceConfigs();
                } catch (Throwable e) {
                    logger.error("scan failed", e);
                }
            }

        }, 10, 3, TimeUnit.SECONDS);
    }

    /**
     * 销毁
     */
    public void destroy() {
        executor.shutdownNow();
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    /**
     * 获取conf文件夹所在路径
     * 
     * @return 路径地址
     */
    private String getConfPath() {
        String classpath = this.getClass().getResource("/").getPath();
        String confPath = classpath + ".." + File.separator + "conf" + File.separator;
        if (new File(confPath).exists()) {
            return confPath;
        } else {
            return classpath;
        }
    }

    /**
     * 配置对应对象
     */
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

    public interface Listener<Properties> {

        void onChange(Properties properties);
    }
}
