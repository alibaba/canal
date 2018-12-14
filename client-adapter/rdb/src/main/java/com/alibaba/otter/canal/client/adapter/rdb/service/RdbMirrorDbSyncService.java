package com.alibaba.otter.canal.client.adapter.rdb.service;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.rdb.support.SingleDml;
import com.alibaba.otter.canal.client.adapter.support.Dml;

/**
 * RDB镜像库同步操作业务
 *
 * @author rewerma 2018-12-12 下午011:23
 * @version 1.0.0
 */
public class RdbMirrorDbSyncService {

    private static final Logger              logger             = LoggerFactory.getLogger(RdbMirrorDbSyncService.class);

    private Map<String, MappingConfig>       mirrorDbConfigCache;                                                       // 镜像库配置
    private DataSource                       dataSource;
    private RdbSyncService                   rdbSyncService;                                                            // rdbSyncService代理

    private final Map<String, MappingConfig> tableDbConfigCache = new ConcurrentHashMap<>();                            // 自动生成的库表配置缓存

    public RdbMirrorDbSyncService(Map<String, MappingConfig> mirrorDbConfigCache, DataSource dataSource,
                                  Integer threads, Map<String, Map<String, Integer>> columnsTypeCache){
        this.mirrorDbConfigCache = mirrorDbConfigCache;
        this.dataSource = dataSource;
        this.rdbSyncService = new RdbSyncService(dataSource, threads, columnsTypeCache);
    }

    /**
     * 批量同步方法
     *
     * @param dmls 批量 DML
     */
    public void sync(List<Dml> dmls) {
        try {
            List<Dml> dmlList = new ArrayList<>();
            for (Dml dml : dmls) {
                String destination = StringUtils.trimToEmpty(dml.getDestination());
                String database = dml.getDatabase();
                MappingConfig configMap = mirrorDbConfigCache.get(destination + "." + database);
                if (configMap == null) {
                    continue;
                }
                if (dml.getSql() != null) {
                    // DDL
                    executeDdl(dml);
                    rdbSyncService.getColumnsTypeCache().remove(destination + "." + database + "." + dml.getTable());
                    tableDbConfigCache.remove(destination + "." + database + "." + dml.getTable()); // 删除对应库表配置
                } else {
                    // DML
                    initMappingConfig(destination + "." + database + "." + dml.getTable(), configMap, dml);
                    dmlList.add(dml);
                }
            }
            if (!dmlList.isEmpty()) {
                rdbSyncService.sync(dmlList, dml -> {
                    String destination = StringUtils.trimToEmpty(dml.getDestination());
                    String database = dml.getDatabase();
                    String table = dml.getTable();
                    MappingConfig config = tableDbConfigCache.get(destination + "." + database + "." + table);

                    if (config == null) {
                        return false;
                    }

                    if (config.getConcurrent()) {
                        List<SingleDml> singleDmls = SingleDml.dml2SingleDmls(dml);
                        singleDmls.forEach(singleDml -> {
                            int hash = rdbSyncService.pkHash(config.getDbMapping(), singleDml.getData());
                            RdbSyncService.SyncItem syncItem = new RdbSyncService.SyncItem(config, singleDml);
                            rdbSyncService.getDmlsPartition()[hash].add(syncItem);
                        });
                    } else {
                        int hash = 0;
                        List<SingleDml> singleDmls = SingleDml.dml2SingleDmls(dml);
                        singleDmls.forEach(singleDml -> {
                            RdbSyncService.SyncItem syncItem = new RdbSyncService.SyncItem(config, singleDml);
                            rdbSyncService.getDmlsPartition()[hash].add(syncItem);
                        });
                    }
                    return true;
                });
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 初始化表配置
     *
     * @param key 配置key: destination.database.table
     * @param baseConfigMap db sync config
     * @param dml DML
     */
    private void initMappingConfig(String key, MappingConfig baseConfigMap, Dml dml) {
        MappingConfig mappingConfig = tableDbConfigCache.get(key);
        if (mappingConfig == null) {
            // 构造一个配置
            mappingConfig = new MappingConfig();
            mappingConfig.setDataSourceKey(baseConfigMap.getDataSourceKey());
            mappingConfig.setDestination(baseConfigMap.getDestination());
            mappingConfig.setOuterAdapterKey(baseConfigMap.getOuterAdapterKey());
            mappingConfig.setConcurrent(baseConfigMap.getConcurrent());
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

            tableDbConfigCache.put(key, mappingConfig);
        }
    }

    /**
     * DDL 操作
     *
     * @param ddl DDL
     */
    private void executeDdl(Dml ddl) {
        try (Connection conn = dataSource.getConnection(); Statement statement = conn.createStatement()) {
            statement.execute(ddl.getSql());
            // 移除对应配置
            tableDbConfigCache.remove(ddl.getDatabase() + "." + ddl.getDatabase() + "." + ddl.getTable());
            if (logger.isTraceEnabled()) {
                logger.trace("Execute DDL sql: {} for database: {}", ddl.getSql(), ddl.getDatabase());
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}
