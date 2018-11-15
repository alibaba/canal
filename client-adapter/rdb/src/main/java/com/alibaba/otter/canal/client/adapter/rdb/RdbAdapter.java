package com.alibaba.otter.canal.client.adapter.rdb;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.rdb.config.ConfigLoader;
import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.rdb.service.RdbEtlService;
import com.alibaba.otter.canal.client.adapter.rdb.service.RdbSyncService;
import com.alibaba.otter.canal.client.adapter.support.*;

@SPI("rdb")
public class RdbAdapter implements OuterAdapter {

    private static Logger              logger             = LoggerFactory.getLogger(RdbAdapter.class);

    private Map<String, MappingConfig> rdbMapping         = new HashMap<>();                          // 文件名对应配置
    private Map<String, MappingConfig> mappingConfigCache = new HashMap<>();                          // 库名-表名对应配置

    private DruidDataSource            dataSource;

    private RdbSyncService             rdbSyncService;

    private int                        commitSize         = 3000;

    @Override
    public void init(OuterAdapterConfig configuration) {
        Map<String, MappingConfig> rdbMappingTmp = ConfigLoader.load();
        // 过滤不匹配的key的配置
        rdbMappingTmp.forEach((key, mappingConfig) -> {
            if ((mappingConfig.getOuterAdapterKey() == null && configuration.getKey() == null)
                || (mappingConfig.getOuterAdapterKey() != null
                    && mappingConfig.getOuterAdapterKey().equalsIgnoreCase(configuration.getKey()))) {
                rdbMapping.put(key, mappingConfig);
            }
        });
        for (MappingConfig mappingConfig : rdbMapping.values()) {
            mappingConfigCache
                .put(StringUtils.trimToEmpty(mappingConfig.getDestination()) + "."
                     + mappingConfig.getDbMapping().getDatabase() + "." + mappingConfig.getDbMapping().getTable(),
                    mappingConfig);
        }

        Map<String, String> properties = configuration.getProperties();
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName(properties.get("jdbc.driverClassName"));
        dataSource.setUrl(properties.get("jdbc.url"));
        dataSource.setUsername(properties.get("jdbc.username"));
        dataSource.setPassword(properties.get("jdbc.password"));
        dataSource.setInitialSize(1);
        dataSource.setMinIdle(1);
        dataSource.setMaxActive(20);
        dataSource.setMaxWait(60000);
        dataSource.setTimeBetweenEvictionRunsMillis(60000);
        dataSource.setMinEvictableIdleTimeMillis(300000);

        try {
            dataSource.init();
        } catch (SQLException e) {
            logger.error("ERROR ## failed to initial datasource: " + properties.get("jdbc.url"), e);
        }

        String threads = properties.get("threads");
        String commitSize = properties.get("commitSize");
        if (commitSize != null) {
            this.commitSize = Integer.valueOf(commitSize);
        }
        rdbSyncService = new RdbSyncService(this.commitSize,
            threads != null ? Integer.valueOf(threads) : null,
            dataSource);
    }

    private AtomicInteger   batchRowNum = new AtomicInteger(0);
    private List<Dml>       dmlList     = Collections.synchronizedList(new ArrayList<>());
    private Lock            syncLock    = new ReentrantLock();
    private Condition       condition   = syncLock.newCondition();
    private ExecutorService executor    = Executors.newFixedThreadPool(1);

    @Override
    public void sync(Dml dml) {
        boolean first = batchRowNum.get() == 0;
        int currentSize = batchRowNum.addAndGet(dml.getData().size());
        dmlList.add(dml);

        if (first) {
            // 开启超时判断
            executor.submit(() -> {
                try {
                    syncLock.lock();
                    if (!condition.await(5, TimeUnit.SECONDS)) {
                        // 批量超时
                        sync();
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                } finally {
                    syncLock.unlock();
                }
            });
        }

        if (currentSize > commitSize) {
            sync();
        }
    }

    private void sync() {
        try {
            syncLock.lock();
            rdbSyncService.sync(mappingConfigCache, dmlList);
            batchRowNum.set(0);
            dmlList.clear();
            condition.signal();
        } finally {
            syncLock.unlock();
        }
    }

    @Override
    public EtlResult etl(String task, List<String> params) {
        EtlResult etlResult = new EtlResult();
        MappingConfig config = rdbMapping.get(task);
        if (config != null) {
            DataSource srcDataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
            if (srcDataSource != null) {
                return RdbEtlService.importData(srcDataSource, dataSource, config, params);
            } else {
                etlResult.setSucceeded(false);
                etlResult.setErrorMessage("DataSource not found");
                return etlResult;
            }
        } else {
            StringBuilder resultMsg = new StringBuilder();
            boolean resSucc = true;
            // ds不为空说明传入的是destination
            for (MappingConfig configTmp : rdbMapping.values()) {
                // 取所有的destination为task的配置
                if (configTmp.getDestination().equals(task)) {
                    DataSource srcDataSource = DatasourceConfig.DATA_SOURCES.get(configTmp.getDataSourceKey());
                    if (srcDataSource == null) {
                        continue;
                    }
                    EtlResult etlRes = RdbEtlService.importData(srcDataSource, dataSource, configTmp, params);
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

    @Override
    public Map<String, Object> count(String task) {
        MappingConfig config = rdbMapping.get(task);
        MappingConfig.DbMapping dbMapping = config.getDbMapping();
        String sql = "SELECT COUNT(1) AS cnt FROM " + dbMapping.getTargetTable();
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
        res.put("targetTable", dbMapping.getTargetTable());

        return res;
    }

    @Override
    public String getDestination(String task) {
        MappingConfig config = rdbMapping.get(task);
        if (config != null) {
            return config.getDestination();
        }
        return null;
    }

    @Override
    public void destroy() {
        executor.shutdown();

        if (rdbSyncService != null) {
            rdbSyncService.close();
        }

        if (dataSource != null) {
            dataSource.close();
        }
    }
}
