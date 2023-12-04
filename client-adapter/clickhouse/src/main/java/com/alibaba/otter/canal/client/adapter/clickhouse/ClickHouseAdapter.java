package com.alibaba.otter.canal.client.adapter.clickhouse;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.filter.stat.StatFilter;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.clickhouse.config.ConfigLoader;
import com.alibaba.otter.canal.client.adapter.clickhouse.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.clickhouse.config.MirrorDbConfig;
import com.alibaba.otter.canal.client.adapter.clickhouse.monitor.ClickHouseConfigMonitor;
import com.alibaba.otter.canal.client.adapter.clickhouse.service.ClickHouseBatchSyncService;
import com.alibaba.otter.canal.client.adapter.clickhouse.service.ClickHouseEtlService;
import com.alibaba.otter.canal.client.adapter.clickhouse.service.ClickHouseMirrorDbBatchSyncService;
import com.alibaba.otter.canal.client.adapter.clickhouse.support.SyncUtil;
import com.alibaba.otter.canal.client.adapter.support.*;

/**
 * ClickHouse Adapter implementation
 *
 * @author: Xander
 * @date: Created in 2023/11/10 1:13
 * @email: zhrunxin33@gmail.com
 */
@SPI
public class ClickHouseAdapter implements OuterAdapter {

    private static final Logger                         logger = LoggerFactory.getLogger(ClickHouseAdapter.class);

    // Store the mapping of filename and configuration, load yml files below
    // resource path
    private Map<String, MappingConfig>              clickHouseMapping   = new ConcurrentHashMap<>();

    // Schema -> Table -> MappingConfig
    private Map<String, Map<String, MappingConfig>> mappingConfigCache  = new ConcurrentHashMap<>();

    // Mirror DB Configuration, don't need to load column mapping
    private Map<String, MirrorDbConfig>             mirrorDbConfigCache = new ConcurrentHashMap<>();

    private DruidDataSource                             dataSource;

    private ClickHouseBatchSyncService clickHouseBatchSyncService;

    private ClickHouseMirrorDbBatchSyncService clickHouseMirrorDbBatchSyncService;

    private Properties                                  envProperties;

    // Launch configuration
    private OuterAdapterConfig                      configuration;

    private ClickHouseConfigMonitor clickHouseConfigMonitor;

    public Map<String, MappingConfig> getClickHouseMapping() {
        return clickHouseMapping;
    }

    @Override
    public void init(OuterAdapterConfig configuration, Properties envProperties) {
        this.envProperties = envProperties;
        this.configuration = configuration;
        // Load DB type from adapter.launch/bootstrap.yml
        Map<String, String> properties = configuration.getProperties();
        String dbType = JdbcUtils.getDbType(properties.get("jdbc.url"), null);
        // 当.yml文件编码格式存在问题，此处clickhouse yml文件构建 可能会抛出异常
        Map<String, MappingConfig> clickHouseMappingTmp = ConfigLoader.load(envProperties);
        // 过滤不匹配的key的配置
        clickHouseMappingTmp.forEach((key, config) -> {
            addConfig(key, config);
        });

        if (clickHouseMapping.isEmpty()) {
            throw new RuntimeException("No clickhouse adapter found for config key: " + configuration.getKey());
        }

        // 初始化连接池
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName(properties.get("jdbc.driverClassName"));
        dataSource.setUrl(properties.get("jdbc.url"));
        dataSource.setUsername(properties.get("jdbc.username"));
        dataSource.setPassword(properties.get("jdbc.password"));
        dataSource.setInitialSize(1);
        dataSource.setMinIdle(1);
        dataSource.setMaxActive(30);
        dataSource.setMaxWait(60000);
        dataSource.setTimeBetweenEvictionRunsMillis(60000);
        dataSource.setMinEvictableIdleTimeMillis(300000);
        dataSource.setUseUnfairLock(true);
        dataSource.setDefaultAutoCommit(false); // disable auto commit or disable Transactional
        dataSource.setDbType(dbType);

        if ("true".equals(properties.getOrDefault("druid.stat.enable", "true"))) {
            StatFilter statFilter = new StatFilter();
            statFilter.setSlowSqlMillis(Long.parseLong(properties.getOrDefault("druid.stat.slowSqlMillis", "1000")));
            statFilter.setMergeSql(true);
            statFilter.setLogSlowSql(true);
            dataSource.setProxyFilters(Collections.singletonList(statFilter));
        }

        try {
            dataSource.init();
        } catch (SQLException e) {
            logger.error("ERROR ## failed to initial datasource: " + properties.get("jdbc.url"), e);
        }

        String threads = properties.get("threads");
        String scheduleTime = properties.get("scheduleTime");
        String batchSize = properties.get("batchSize");

        boolean skipDupException = BooleanUtils.toBoolean(configuration.getProperties()
                .getOrDefault("skipDupException", "true"));
        clickHouseBatchSyncService = new ClickHouseBatchSyncService(dataSource,
                threads != null ? Integer.valueOf(threads) : null,
                batchSize != null ? Integer.valueOf(batchSize) : null,
                scheduleTime != null ? Long.valueOf(scheduleTime) : null,
                skipDupException);

        clickHouseMirrorDbBatchSyncService = new ClickHouseMirrorDbBatchSyncService(mirrorDbConfigCache,
                dataSource,
                threads != null ? Integer.valueOf(threads) : null,
                batchSize != null ? Integer.valueOf(batchSize) : null,
                scheduleTime != null ? Long.valueOf(scheduleTime) : null,
                clickHouseBatchSyncService.getColumnsTypeCache(),
                skipDupException);

        clickHouseConfigMonitor = new ClickHouseConfigMonitor();
        clickHouseConfigMonitor.init(configuration.getKey(), this, envProperties);
    }

    /**
     * Sync main entrance
     *
     * @param dmls 数据包
     */
    @Override
    public void sync(List<Dml> dmls) {
        if (dmls == null || dmls.isEmpty()) {
            return;
        }
        try {
            // If mappingConfigCache(column mapping) is empty, that must be mirroring synchronize
            if (!mappingConfigCache.isEmpty()) {
                clickHouseBatchSyncService.sync(mappingConfigCache, dmls, envProperties);
            }
            clickHouseMirrorDbBatchSyncService.sync(dmls);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Destroy
     */
    @Override
    public void destroy() {
        if (clickHouseConfigMonitor != null) {
            clickHouseConfigMonitor.destroy();
        }

        if (clickHouseBatchSyncService != null) {
            clickHouseBatchSyncService.close();
        }

        if (dataSource != null) {
            dataSource.close();
        }
    }


    /**
     * ETL方法
     *
     * @param task 任务名, 对应配置名
     * @param params etl筛选条件
     * @return ETL结果
     */
    @Override
    public EtlResult etl(String task, List<String> params) {
        EtlResult etlResult = new EtlResult();
        MappingConfig config = clickHouseMapping.get(task);
        ClickHouseEtlService clickhouseEtlService = new ClickHouseEtlService(dataSource, config);
        if (config != null) {
            return clickhouseEtlService.importData(params);
        } else {
            StringBuilder resultMsg = new StringBuilder();
            boolean resSucc = true;
            for (MappingConfig configTmp : clickHouseMapping.values()) {
                // 取所有的destination为task的配置
                if (configTmp.getDestination().equals(task)) {
                    EtlResult etlRes = clickhouseEtlService.importData(params);
                    if (!etlRes.getSucceeded()) {
                        resSucc = false;
                        resultMsg.append(etlRes.getErrorMessage()).append("\n");
                    } else {
                        resultMsg.append(etlRes.getResultMessage()).append("\n");
                    }
                }
            }
            if (resultMsg.length() > 0) {
                etlResult.setSucceeded(resSucc);
                if (resSucc) {
                    etlResult.setResultMessage(resultMsg.toString());
                } else {
                    etlResult.setErrorMessage(resultMsg.toString());
                }
                return etlResult;
            }
        }
        etlResult.setSucceeded(false);
        etlResult.setErrorMessage("Task not found");
        return etlResult;
    }

    /**
     * 获取总数方法
     *
     * @param task 任务名, 对应配置名
     * @return 总数
     */
    @Override
    public Map<String, Object> count(String task) {
        MappingConfig config = clickHouseMapping.get(task);
        MappingConfig.DbMapping dbMapping = config.getDbMapping();
        String sql = "SELECT COUNT(1) AS cnt FROM " + SyncUtil.getDbTableName(dbMapping, dataSource.getDbType());
        Connection conn = null;
        Map<String, Object> res = new LinkedHashMap<>();
        try {
            conn = dataSource.getConnection();
            Util.sqlRS(conn, sql, rs -> {
                try {
                    if (rs.next()) {
                        Long rowCount = rs.getLong("cnt");
                        res.put("count", rowCount);
                    }
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                }
            });
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
        res.put("targetTable", SyncUtil.getDbTableName(dbMapping, dataSource.getDbType()));

        return res;
    }

    /**
     * 获取对应canal instance name 或 mq topic
     *
     * @param task 任务名, 对应配置名
     * @return destination
     */
    @Override
    public String getDestination(String task) {
        MappingConfig config = clickHouseMapping.get(task);
        if (config != null) {
            return config.getDestination();
        }
        return null;
    }

    private void addSyncConfigToCache(String configName, MappingConfig mappingConfig) {
        if (!mappingConfig.getDbMapping().getMirrorDb()) {
            String key;
            if (envProperties != null && !"tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
                key = StringUtils.trimToEmpty(mappingConfig.getDestination()) + "-"
                        + StringUtils.trimToEmpty(mappingConfig.getGroupId()) + "_"
                        + mappingConfig.getDbMapping().getDatabase() + "-" + mappingConfig.getDbMapping().getTable();
            } else {
                key = StringUtils.trimToEmpty(mappingConfig.getDestination()) + "_"
                        + mappingConfig.getDbMapping().getDatabase() + "-" + mappingConfig.getDbMapping().getTable();
            }
            Map<String, MappingConfig> configMap = mappingConfigCache.computeIfAbsent(key,
                    k1 -> new ConcurrentHashMap<>());
            configMap.put(configName, mappingConfig);
        } else {
            // mirrorDB
            String key = StringUtils.trimToEmpty(mappingConfig.getDestination()) + "."
                    + mappingConfig.getDbMapping().getDatabase();
            mirrorDbConfigCache.put(key, MirrorDbConfig.create(configName, mappingConfig));
        }
    }

    public boolean addConfig(String fileName, MappingConfig config) {
        if (match(config)) {
            clickHouseMapping.put(fileName, config);
            addSyncConfigToCache(fileName, config);
            FileName2KeyMapping.register(getClass().getAnnotation(SPI.class).value(), fileName,
                    configuration.getKey());
            return true;
        }
        return false;
    }

    public void updateConfig(String fileName, MappingConfig config) {
        if (config.getOuterAdapterKey() != null && !config.getOuterAdapterKey()
                .equals(configuration.getKey())) {
            // 理论上不允许改这个 因为本身就是通过这个关联起Adapter和Config的
            throw new RuntimeException("not allow to change outAdapterKey");
        }
        clickHouseMapping.put(fileName, config);
        addSyncConfigToCache(fileName, config);
    }

    public void deleteConfig(String fileName) {
        clickHouseMapping.remove(fileName);
        for (Map<String, MappingConfig> configMap : mappingConfigCache.values()) {
            if (configMap != null) {
                configMap.remove(fileName);
            }
        }
        FileName2KeyMapping.unregister(getClass().getAnnotation(SPI.class).value(), fileName);
    }

    private boolean match(MappingConfig config) {
        boolean sameMatch = config.getOuterAdapterKey() != null && config.getOuterAdapterKey()
                .equalsIgnoreCase(configuration.getKey());
        boolean prefixMatch = config.getOuterAdapterKey() == null && configuration.getKey()
                .startsWith(StringUtils
                        .join(new String[]{Util.AUTO_GENERATED_PREFIX, config.getDestination(),
                                config.getGroupId()}, '-'));
        return sameMatch || prefixMatch;
    }
}
