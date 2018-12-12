package com.alibaba.otter.canal.client.adapter.rdb.service;

import java.sql.Connection;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;

public class RdbMirrorDbSyncService {

    private static final Logger               logger             = LoggerFactory
        .getLogger(RdbMirrorDbSyncService.class);

    private Map<String, MappingConfig>        mirrorDbConfigCache;                           // 镜像库配置
    private DataSource                        dataSource;

    private static Map<String, MappingConfig> tableDbConfigCache = new ConcurrentHashMap<>();

    public RdbMirrorDbSyncService(Map<String, MappingConfig> mirrorDbConfigCache, DataSource dataSource,
                                  Integer threads){
        this.mirrorDbConfigCache = mirrorDbConfigCache;
        this.dataSource = dataSource;
    }

    public void sync(List<Dml> dmls) {
        for (Dml dml : dmls) {
            String destination = StringUtils.trimToEmpty(dml.getDestination());
            String database = dml.getDatabase();
            MappingConfig configMap = mirrorDbConfigCache.get(destination + "." + database);
            if (configMap == null) {
                continue;
            }
            if (dml.getSql() != null) {
                // DDL
                executeDdl(database, dml.getSql());
            } else {
                // DML
                // TODO
                MappingConfig mappingConfig = tableDbConfigCache
                    .get(destination + "." + database + "." + dml.getTable());
                if (mappingConfig == null) {
                    // 构造一个配置
                    mappingConfig = new MappingConfig();
                    mappingConfig.setDataSourceKey(configMap.getDataSourceKey());
                    mappingConfig.setDestination(configMap.getDestination());
                    mappingConfig.setOuterAdapterKey(configMap.getOuterAdapterKey());
                    mappingConfig.setConcurrent(configMap.getConcurrent());
                    MappingConfig.DbMapping dbMapping = new MappingConfig.DbMapping();
                    mappingConfig.setDbMapping(dbMapping);
                    dbMapping.setDatabase(dml.getDatabase());
                    dbMapping.setTable(dml.getTable());
                    dbMapping.setTargetDb(dml.getDatabase());
                    dbMapping.setTargetTable(dml.getTable());
                    dbMapping.setMapAll(true);
                    List<String> pkNames = dml.getPkNames();
                    Map<String, String> pkMapping = new LinkedHashMap<>();
                    pkNames.forEach(pkName -> pkMapping.put(pkName, pkName));
                    dbMapping.setTargetPk(pkMapping);

                    tableDbConfigCache.put(destination + "." + database + "." + dml.getTable(), mappingConfig);
                }
            }
        }
    }

    private void executeDdl(String database, String sql) {
        try (Connection conn = dataSource.getConnection(); Statement statement = conn.createStatement()) {
            statement.execute(sql);
            if (logger.isTraceEnabled()) {
                logger.trace("Execute DDL sql: {} for database: {}", sql, database);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}
