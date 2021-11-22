package com.alibaba.otter.canal.client.adapter.phoenix;
import com.alibaba.otter.canal.client.adapter.phoenix.config.ConfigLoader;
import com.alibaba.otter.canal.client.adapter.phoenix.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.phoenix.monitor.PhoenixConfigMonitor;
import com.alibaba.otter.canal.client.adapter.phoenix.service.PhoenixEtlService;
import com.alibaba.otter.canal.client.adapter.phoenix.service.PhoenixSyncService;
import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.phoenix.support.SyncUtil;
import com.alibaba.otter.canal.client.adapter.support.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: lihua
 * @date: 2021/1/5 15:01
 * @Description: Phoenix适配器实现类
 */
@SPI("phoenix")
public class PhoenixAdapter implements OuterAdapter {

    private static Logger logger = LoggerFactory.getLogger(PhoenixAdapter.class);

    private Map<String, MappingConfig> phoenixMapping = new ConcurrentHashMap<>();                // 文件名对应配置
    private Map<String, Map<String, MappingConfig>> mappingConfigCache = new ConcurrentHashMap<>();                // 库名-表名对应配置

    private static  String DriverClass;
    private static  String PhoenixUrl;
    private static  Properties phoenixPro = new Properties();

    private PhoenixSyncService phoenixSyncService;

    private PhoenixConfigMonitor phoenixConfigMonitor;

    private Properties envProperties;

    private OuterAdapterConfig configuration;

    public Map<String, MappingConfig> getPhoenixMapping() {
        return phoenixMapping;
    }

    public Map<String, Map<String, MappingConfig>> getMappingConfigCache() {
        return mappingConfigCache;
    }

    public PhoenixAdapter() {
        logger.info("PhoenixAdapter create: {} {}", this, Thread.currentThread().getStackTrace());
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
        Map<String, MappingConfig> phoenixMappingTmp = ConfigLoader.load(envProperties);
        // 过滤不匹配的key的配置
        phoenixMappingTmp.forEach((key, config) -> {
            addConfig(key, config);
        });

        if (phoenixMapping.isEmpty()) {
            throw new RuntimeException("No phoenix adapter found for config key: " + configuration.getKey());
        } else {
            logger.info("[{}]phoenix config mapping: {}", this, phoenixMapping.keySet());
        }

        Map<String, String> properties = configuration.getProperties();

        DriverClass= properties.get("jdbc.driverClassName");
        PhoenixUrl=properties.get("jdbc.url");

        try {
            //phoenix内部本身有连接池，不需要使用Druid初始化
            phoenixPro.setProperty("hbase.rpc.timeout","600000");
            phoenixPro.setProperty("hbase.client.scanner.timeout.period","600000");
            phoenixPro.setProperty("dfs.client.socket-timeout","600000");
            phoenixPro.setProperty("phoenix.query.keepAliveMs","600000");
            phoenixPro.setProperty("phoenix.query.timeoutMs","3600000");
            Class.forName(DriverClass);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        String threads = properties.get("threads");
        phoenixSyncService = new PhoenixSyncService(
                threads != null ? Integer.valueOf(threads) : null
        );

        phoenixConfigMonitor = new PhoenixConfigMonitor();
        phoenixConfigMonitor.init(configuration.getKey(), this, envProperties);
    }


    /**
     * 获取phoenix连接
     * @return
     */
    public static  Connection  getPhoenixConnection() {
        try {
            return DriverManager.getConnection(PhoenixUrl,phoenixPro);
        } catch (SQLException e) {
            logger.error("getPhoenixConnection Exception"+e.getMessage());
        }
        return  null;
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
            phoenixSyncService.sync(mappingConfigCache, dmls, envProperties);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * ETL方法
     *
     * @param task   任务名, 对应配置名
     * @param params etl筛选条件
     * @return ETL结果
     */
    @Override
    public EtlResult etl(String task, List<String> params) {
        EtlResult etlResult = new EtlResult();
        MappingConfig config = phoenixMapping.get(task);
        if (config != null) {
            DataSource srcDataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
            if (srcDataSource != null) {
                return PhoenixEtlService.importData(srcDataSource, getPhoenixConnection(), config, params);
            } else {
                etlResult.setSucceeded(false);
                etlResult.setErrorMessage("DataSource not found");
                return etlResult;
            }
        } else {
            StringBuilder resultMsg = new StringBuilder();
            boolean resSucc = true;
            // ds不为空说明传入的是destination
            for (MappingConfig configTmp : phoenixMapping.values()) {
                // 取所有的destination为task的配置
                if (configTmp.getDestination().equals(task)) {
                    DataSource srcDataSource = DatasourceConfig.DATA_SOURCES.get(configTmp.getDataSourceKey());
                    if (srcDataSource == null) {
                        continue;
                    }
                    EtlResult etlRes = PhoenixEtlService.importData(srcDataSource,getPhoenixConnection(), configTmp, params);
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
        Map<String, Object> res = new LinkedHashMap<>();
        MappingConfig config = phoenixMapping.get(task);
        if (config == null) {
            logger.info("[{}]phoenix config mapping: {}", this, phoenixMapping.keySet());
            res.put("succeeded", false);
            res.put("errorMessage", "Task[" + task + "] not found");
            res.put("tasks", phoenixMapping.keySet());
            return res;
        }
        MappingConfig.DbMapping dbMapping = config.getDbMapping();
        String sql = "SELECT COUNT(1) AS cnt FROM " + SyncUtil.getDbTableName(dbMapping);
        Connection conn = null;
        try {
            //conn = dataSource.getConnection();
            conn = getPhoenixConnection();
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
        } catch (Exception e) {
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
        res.put("targetTable", SyncUtil.getDbTableName(dbMapping));

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
        MappingConfig config = phoenixMapping.get(task);
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
        if (phoenixConfigMonitor != null) {
            phoenixConfigMonitor.destroy();
        }

        if (phoenixSyncService != null) {
            phoenixSyncService.close();
        }
    }

    private void addSyncConfigToCache(String configName, MappingConfig mappingConfig) {
        String key;
        if (envProperties != null && !"tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
            key = StringUtils.trimToEmpty(mappingConfig.getDestination()) + "-"
                    + StringUtils.trimToEmpty(mappingConfig.getGroupId()) + "_"
                    + mappingConfig.getDbMapping().getDatabase() + "-" + mappingConfig.getDbMapping().getTable().toLowerCase();
        } else {
            key = StringUtils.trimToEmpty(mappingConfig.getDestination()) + "_"
                    + mappingConfig.getDbMapping().getDatabase() + "-" + mappingConfig.getDbMapping().getTable().toLowerCase();
        }
        Map<String, MappingConfig> configMap = mappingConfigCache.computeIfAbsent(key,
                k1 -> new ConcurrentHashMap<>());
        configMap.put(configName, mappingConfig);
    }

    public boolean addConfig(String fileName, MappingConfig config) {
        if (match(config)) {
            phoenixMapping.put(fileName, config);
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
        phoenixMapping.put(fileName, config);
        addSyncConfigToCache(fileName, config);
    }

    public void deleteConfig(String fileName) {
        phoenixMapping.remove(fileName);
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
