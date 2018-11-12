package com.alibaba.otter.canal.client.adapter.rdb;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.rdb.config.ConfigLoader;
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

    @Override
    public void init(OuterAdapterConfig configuration) {
        System.out.println("xxxxx: " + this.getClass().getClassLoader().getResource("").getPath());
        File file = new File(this.getClass().getClassLoader().getResource("").getPath() + "rdb" + File.separator);
        System.out.println(file.getAbsolutePath());

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
        dataSource.setMaxActive(2);
        dataSource.setMaxWait(60000);
        dataSource.setTimeBetweenEvictionRunsMillis(60000);
        dataSource.setMinEvictableIdleTimeMillis(300000);

        try {
            dataSource.init();
        } catch (SQLException e) {
            logger.error("ERROR ## failed to initial datasource: " + properties.get("jdbc.url"), e);
        }

        rdbMapping.values().forEach(config -> {
            try {
                MappingConfig.DbMapping dbMapping = config.getDbMapping();
                // 从源表加载所有字段名
                if (dbMapping.getAllColumns() == null) {
                    synchronized (RdbSyncService.class) {
                        if (dbMapping.getAllColumns() == null) {
                            DataSource srcDS = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
                            Connection srcConn = srcDS.getConnection();
                            String srcMetaSql = "SELECT * FROM " + dbMapping.getDatabase() + "." + dbMapping.getTable()
                                                + " WHERE 1=2 ";
                            List<String> srcColumns = new ArrayList<>();
                            Util.sqlRS(srcConn, srcMetaSql, rs -> {
                                try {
                                    ResultSetMetaData rmd = rs.getMetaData();
                                    int cnt = rmd.getColumnCount();
                                    for (int i = 1; i <= cnt; i++) {
                                        srcColumns.add(rmd.getColumnName(i).toLowerCase());
                                    }
                                } catch (SQLException e) {
                                    logger.error(e.getMessage(), e);
                                }
                            });
                            Map<String, String> columnsMap = new LinkedHashMap<>();

                            for (String srcColumn : srcColumns) {
                                String targetColumn = srcColumn;
                                if (dbMapping.getTargetColumns() != null) {
                                    for (Map.Entry<String, String> entry : dbMapping.getTargetColumns().entrySet()) {
                                        String targetColumnName = entry.getKey();
                                        String srcColumnName = entry.getValue();

                                        if (srcColumnName != null
                                            && srcColumnName.toLowerCase().equals(srcColumn.toUpperCase())) {
                                            targetColumn = targetColumnName;
                                        }
                                    }
                                }
                                columnsMap.put(targetColumn, srcColumn);
                            }
                            dbMapping.setAllColumns(columnsMap);
                        }
                    }
                }
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
            }
        });

        rdbSyncService = new RdbSyncService(dataSource);
    }

    @Override
    public void sync(Dml dml) {
        String destination = StringUtils.trimToEmpty(dml.getDestination());
        String database = dml.getDatabase();
        String table = dml.getTable();
        MappingConfig config = mappingConfigCache.get(destination + "." + database + "." + table);

        rdbSyncService.sync(config, dml);
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
        if (dataSource != null) {
            dataSource.close();
        }
    }
}
