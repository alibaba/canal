package com.alibaba.otter.canal.adapter.launcher.monitor.remote;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.adapter.launcher.config.AdapterConfigHolder;
import com.alibaba.otter.canal.common.utils.CommonUtils;
import com.alibaba.otter.canal.common.utils.NamedThreadFactory;
import com.google.common.base.Joiner;

/**
 * 基于数据库的远程配置装载器
 *
 * @author rewerma 2019-01-25 下午05:20:16
 * @version 1.0.0
 */
public class DbRemoteConfigLoader implements RemoteConfigLoader {

    private static final Logger      logger                    = LoggerFactory.getLogger(DbRemoteConfigLoader.class);

    private DruidDataSource          dataSource;

    private AdapterConfigHolder      remoteAdapterConfigHolder = AdapterConfigHolder.getInstance();

    private ScheduledExecutorService executor                  = Executors.newScheduledThreadPool(2,
        new NamedThreadFactory("remote-adapter-config-scan"));

    private RemoteAdapterMonitor     remoteAdapterMonitor      = new RemoteAdapterMonitorImpl();

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
     * 加载远程application.yml配置
     */
    @Override
    public void loadRemoteConfig() {
        try {
            // 加载远程adapter配置
            ConfigItem configItem = getRemoteAdapterConfig();
            if (configItem != null) {
                if (configItem.getModifiedTime() != remoteAdapterConfigHolder.getAdapterConfigTimestamp()) {
                    remoteAdapterConfigHolder.setAdapterConfigTimestamp(configItem.getModifiedTime());
                    overrideLocalCanalConfig(configItem.getContent());
                    logger.info("## Loaded remote adapter config: application.yml");
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 获取远程application.yml配置
     *
     * @return 配置对象
     */
    private ConfigItem getRemoteAdapterConfig() {
        String sql = "select name, content, modified_time from canal_config where id=2";
        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
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

    /**
     * 覆盖本地application.yml文件
     *
     * @param content 文件内容
     */
    private void overrideLocalCanalConfig(String content) {

        try (OutputStreamWriter writer = new OutputStreamWriter(
            new FileOutputStream(CommonUtils.getConfPath() + "application.yml"),
            StandardCharsets.UTF_8)) {
            writer.write(content);
            writer.flush();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 加载adapter配置
     */
    @Override
    public void loadRemoteAdapterConfigs() {
        try {
            // 加载远程adapter配置
            loadModifiedAdapterConfigs();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 加载有变动的adapter配置
     */
    private void loadModifiedAdapterConfigs() {
        Map<String, ConfigItem> remoteConfigStatus = new HashMap<>();
        String sql = "select id, category, name, modified_time from canal_adapter_config";
        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                ConfigItem configItem = new ConfigItem();
                configItem.setId(rs.getLong("id"));
                configItem.setCategory(rs.getString("category"));
                configItem.setName(rs.getString("name"));
                configItem.setModifiedTime(rs.getTimestamp("modified_time").getTime());
                remoteConfigStatus.put(configItem.getCategory() + "/" + configItem.getName(), configItem);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        if (!remoteConfigStatus.isEmpty()) {
            List<Long> changedIds = new ArrayList<>();

            for (ConfigItem remoteConfigStat : remoteConfigStatus.values()) {
                ConfigItem currentConfig = remoteAdapterConfigHolder.getAdapterConfigs()
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
                String contentsSql = "select id, category, name, content, modified_time from canal_adapter_config  where id in ("
                                     + Joiner.on(",").join(changedIds) + ")";
                try (Connection conn = dataSource.getConnection();
                        Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery(contentsSql)) {
                    while (rs.next()) {
                        ConfigItem configItemNew = new ConfigItem();
                        configItemNew.setId(rs.getLong("id"));
                        configItemNew.setCategory(rs.getString("category"));
                        configItemNew.setName(rs.getString("name"));
                        configItemNew.setContent(rs.getString("content"));
                        configItemNew.setModifiedTime(rs.getTimestamp("modified_time").getTime());

                        remoteAdapterConfigHolder.getAdapterConfigs()
                            .put(configItemNew.getCategory() + "/" + configItemNew.getName(), configItemNew);
                        remoteAdapterMonitor.onModify(configItemNew);
                    }

                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }

        for (ConfigItem configItem : remoteAdapterConfigHolder.getAdapterConfigs().values()) {
            if (!remoteConfigStatus.containsKey(configItem.getCategory() + "/" + configItem.getName())) {
                // 删除
                remoteAdapterConfigHolder.getAdapterConfigs()
                    .remove(configItem.getCategory() + "/" + configItem.getName());
                remoteAdapterMonitor.onDelete(configItem.getCategory() + "/" + configItem.getName());
            }
        }
    }

    /**
     * 启动监听数据库变化
     */
    @Override
    public void startMonitor() {
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

    /**
     * 销毁
     */
    @Override
    public void destroy() {
        executor.shutdownNow();
        try {
            dataSource.close();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}
