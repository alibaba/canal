package com.alibaba.otter.canal.client.adapter.rdb;

import com.alibaba.otter.canal.client.adapter.support.FileName2KeyMapping;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.filter.stat.StatFilter;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.rdb.config.ConfigLoader;
import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.rdb.config.MirrorDbConfig;
import com.alibaba.otter.canal.client.adapter.rdb.monitor.RdbConfigMonitor;
import com.alibaba.otter.canal.client.adapter.rdb.service.RdbEtlService;
import com.alibaba.otter.canal.client.adapter.rdb.service.RdbMirrorDbSyncService;
import com.alibaba.otter.canal.client.adapter.rdb.service.RdbSyncService;
import com.alibaba.otter.canal.client.adapter.rdb.support.SyncUtil;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;
import com.alibaba.otter.canal.client.adapter.support.SPI;
import com.alibaba.otter.canal.client.adapter.support.Util;

/**
 * RDB适配器实现类
 *
 * @author rewerma 2018-11-7 下午06:45:49
 * @version 1.0.0
 */
@SPI("rdb")
public class RdbAdapter implements OuterAdapter {

    private static Logger                           logger              = LoggerFactory.getLogger(RdbAdapter.class);

    private Map<String, MappingConfig>              rdbMapping          = new ConcurrentHashMap<>();                // 文件名对应配置
    private Map<String, Map<String, MappingConfig>> mappingConfigCache  = new ConcurrentHashMap<>();                // 库名-表名对应配置
    private Map<String, MirrorDbConfig>             mirrorDbConfigCache = new ConcurrentHashMap<>();                // 镜像库配置

    private DruidDataSource                         dataSource;

    private RdbSyncService                          rdbSyncService;
    private RdbMirrorDbSyncService                  rdbMirrorDbSyncService;

    private RdbConfigMonitor                        rdbConfigMonitor;

    private Properties                              envProperties;

    private OuterAdapterConfig                      configuration;

    public Map<String, MappingConfig> getRdbMapping() {
        return rdbMapping;
    }

    public Map<String, Map<String, MappingConfig>> getMappingConfigCache() {
        return mappingConfigCache;
    }

    public Map<String, MirrorDbConfig> getMirrorDbConfigCache() {
        return mirrorDbConfigCache;
    }

    /**
     * 初始化方法
     *
     * @param configuration 外部适配器配置信息
     */
    @Override
    public void init(OuterAdapterConfig configuration, Properties envProperties) {
        this.envProperties = envProperties;
        this.configuration = configuration;
      
        // 从jdbc url获取db类型
        Map<String, String> properties = configuration.getProperties();
        String dbType = JdbcUtils.getDbType(properties.get("jdbc.url"), null);
        Map<String, MappingConfig> rdbMappingTmp = ConfigLoader.load(envProperties);
        // 过滤不匹配的key的配置
        rdbMappingTmp.forEach((key, config) -> {
            addConfig(key, config);
        });

        if (rdbMapping.isEmpty()) {
            throw new RuntimeException("No rdb adapter found for config key: " + configuration.getKey());
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
        dataSource.setDbType(dbType);

        // List<String> array = new ArrayList<>();
        // array.add("set names utf8mb4;");
        // dataSource.setConnectionInitSqls(array);

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
        // String commitSize = properties.get("commitSize");

        boolean skipDupException = BooleanUtils.toBoolean(configuration.getProperties()
            .getOrDefault("skipDupException", "true"));
        rdbSyncService = new RdbSyncService(dataSource,
            threads != null ? Integer.valueOf(threads) : null,
            skipDupException);

        rdbMirrorDbSyncService = new RdbMirrorDbSyncService(mirrorDbConfigCache,
            dataSource,
            threads != null ? Integer.valueOf(threads) : null,
            rdbSyncService.getColumnsTypeCache(),
            skipDupException);

        rdbConfigMonitor = new RdbConfigMonitor();
        rdbConfigMonitor.init(configuration.getKey(), this, envProperties);
    }

    /**
     * 同步方法
     *
     * @param dmls 数据包
     */
    @Override
    public void sync(List<Dml> dmls) {
        if (dmls == null || dmls.isEmpty()) {
            return;
        }
        try {
            if (!mappingConfigCache.isEmpty()) {
                rdbSyncService.sync(mappingConfigCache, dmls, envProperties);
            }
            rdbMirrorDbSyncService.sync(dmls);
        } catch (Exception e) {
            throw new RuntimeException(e);
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
        MappingConfig config = rdbMapping.get(task);
        RdbEtlService rdbEtlService = new RdbEtlService(dataSource, config);
        if (config != null) {
            return rdbEtlService.importData(params);
        } else {
            StringBuilder resultMsg = new StringBuilder();
            boolean resSucc = true;
            for (MappingConfig configTmp : rdbMapping.values()) {
                // 取所有的destination为task的配置
                if (configTmp.getDestination().equals(task)) {
                    EtlResult etlRes = rdbEtlService.importData(params);
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
        MappingConfig config = rdbMapping.get(task);
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
        MappingConfig config = rdbMapping.get(task);
        if (config != null) {
            return config.getDestination();
        }
        return null;
    }

    /**
     * 销毁方法
     */
    @Override
    public void destroy() {
        if (rdbConfigMonitor != null) {
            rdbConfigMonitor.destroy();
        }

        if (rdbSyncService != null) {
            rdbSyncService.close();
        }

        if (dataSource != null) {
            dataSource.close();
        }
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
            rdbMapping.put(fileName, config);
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
        rdbMapping.put(fileName, config);
        addSyncConfigToCache(fileName, config);
    }

    public void deleteConfig(String fileName) {
        rdbMapping.remove(fileName);
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
