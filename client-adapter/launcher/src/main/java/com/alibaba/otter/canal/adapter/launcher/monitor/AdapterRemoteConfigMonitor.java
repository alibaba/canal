package com.alibaba.otter.canal.adapter.launcher.monitor;

import java.io.File;
import java.io.FileWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Joiner;
import com.google.common.collect.MapMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.common.utils.NamedThreadFactory;

public class AdapterRemoteConfigMonitor {

    private static final Logger      logger                 = LoggerFactory.getLogger(AdapterRemoteConfigMonitor.class);

    private Connection               conn;
    private String                   jdbcUrl;
    private String                   jdbcUsername;
    private String                   jdbcPassword;

    private long                     currentConfigTimestamp = 0;
    private Map<String, ConfigItem>  remoteAdapterConfigs   = new MapMaker().makeMap();

    private ScheduledExecutorService executor               = Executors.newScheduledThreadPool(2,
        new NamedThreadFactory("remote-adapter-config-scan"));

    public AdapterRemoteConfigMonitor(String jdbcUrl, String jdbcUsername, String jdbcPassword){
        this.jdbcUrl = jdbcUrl;
        this.jdbcUsername = jdbcUsername;
        this.jdbcPassword = jdbcPassword;
    }

    private Connection getConn() throws Exception {
        if (conn == null || conn.isClosed()) {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
        }
        return conn;
    }

    public void loadRemoteConfig() {
        try {
            // 加载远程adapter配置
            ConfigItem configItem = getRemoteAdapterConfig();
            if (configItem != null) {
                if (configItem.getModifiedTime() != currentConfigTimestamp) {
                    currentConfigTimestamp = configItem.getModifiedTime();
                    overrideLocalCanalConfig(configItem.getContent());
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private ConfigItem getRemoteAdapterConfig() {
        String sql = "select name, content, modified_time from canal_config where id=2";
        try (Statement stmt = getConn().createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                ConfigItem configItem = new ConfigItem();
                configItem.setId(2L);
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
        try (FileWriter writer = new FileWriter(getConfPath() + "application.yml")) {
            writer.write(content);
            writer.flush();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void start() {
        // 监听application.yml变化
        executor.scheduleWithFixedDelay(() -> {
            try {
                loadRemoteConfig();
            } catch (Throwable e) {
                logger.error("scan remote application.yml failed", e);
            }
        }, 10, 3, TimeUnit.SECONDS);

        // 监听adapter变化
        executor.scheduleWithFixedDelay(() -> {
            try {
                loadRemoteAdapterConfigs();
            } catch (Throwable e) {
                logger.error("scan remote adapter configs failed", e);
            }
        }, 10, 3, TimeUnit.SECONDS);
    }

    public void loadRemoteAdapterConfigs() {
        try {
            // 加载远程instance配置
            Map<String, ConfigItem>[] modifiedConfigs = getModifiedAdapterConfigs();
            if (modifiedConfigs != null) {
                overrideLocalAdapterConfigs(modifiedConfigs);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, ConfigItem>[] getModifiedAdapterConfigs() {
        Map<String, ConfigItem>[] res = new Map[2];
        Map<String, ConfigItem> remoteConfigStatus = new HashMap<>();
        String sql = "select id, category, name, modified_time from canal_instance_config";
        try (Statement stmt = getConn().createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                ConfigItem configItem = new ConfigItem();
                configItem.setId(rs.getLong("id"));
                configItem.setCategory(rs.getString("category"));
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
                ConfigItem currentConfig = remoteAdapterConfigs
                    .get(remoteConfigStat.getCategory() + "/" + remoteConfigStat.getName());
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
                        configItemNew.setCategory(rs.getString("category"));
                        configItemNew.setName(rs.getString("name"));
                        configItemNew.setContent(rs.getString("content"));
                        configItemNew.setModifiedTime(rs.getTimestamp("modified_time").getTime());

                        remoteAdapterConfigs.put(configItemNew.getCategory() + "/" + configItemNew.getName(),
                            configItemNew);
                        changedInstanceConfig.put(configItemNew.getCategory() + "/" + configItemNew.getName(),
                            configItemNew);
                    }

                    res[0] = changedInstanceConfig.isEmpty() ? null : changedInstanceConfig;
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }

        Map<String, ConfigItem> removedInstanceConfig = new HashMap<>();
        for (String name : remoteAdapterConfigs.keySet()) {
            if (!remoteConfigStatus.containsKey(name)) {
                // 删除
                remoteAdapterConfigs.remove(name);
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

    private void overrideLocalAdapterConfigs(Map<String, ConfigItem>[] modifiedInstanceConfigs) {
        Map<String, ConfigItem> changedInstanceConfigs = modifiedInstanceConfigs[0];
        if (changedInstanceConfigs != null) {
            for (ConfigItem configItem : changedInstanceConfigs.values()) {
                try (FileWriter writer = new FileWriter(
                    getConfPath() + configItem.getCategory() + "/" + configItem.getName())) {
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
                File file = new File(getConfPath() + name);
                if (file.exists()) {
                    file.delete();
                }
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
        String confPath = classpath + "../conf/";
        if (new File(confPath).exists()) {
            return confPath;
        } else {
            return classpath;
        }
    }

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
     * 配置对应对象
     */
    public static class ConfigItem {

        private Long   id;
        private String category;
        private String name;
        private String content;
        private long   modifiedTime;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getCategory() {
            return category;
        }

        public void setCategory(String category) {
            this.category = category;
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
