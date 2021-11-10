package com.alibaba.otter.canal.client.adapter.rdb.service;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.rdb.config.MirrorDbConfig;
import com.alibaba.otter.canal.client.adapter.rdb.support.SingleDml;
import com.alibaba.otter.canal.client.adapter.support.Dml;

/**
 * RDB镜像库同步操作业务
 *
 * @author rewerma 2018-12-12 下午011:23
 * @version 1.0.0
 */
public class RdbMirrorDbSyncService {

    private static final Logger         logger = LoggerFactory.getLogger(RdbMirrorDbSyncService.class);

    private Map<String, MirrorDbConfig> mirrorDbConfigCache;                                           // 镜像库配置
    private DataSource                  dataSource;
    private RdbSyncService              rdbSyncService;                                                // rdbSyncService代理

    public RdbMirrorDbSyncService(Map<String, MirrorDbConfig> mirrorDbConfigCache, DataSource dataSource,
                                  Integer threads, Map<String, Map<String, Integer>> columnsTypeCache,
                                  boolean skipDupException){
        this.mirrorDbConfigCache = mirrorDbConfigCache;
        this.dataSource = dataSource;
        this.rdbSyncService = new RdbSyncService(dataSource, threads, columnsTypeCache, skipDupException);
    }

    /**
     * 批量同步方法
     *
     * @param dmls 批量 DML
     */
    public void sync(List<Dml> dmls) {
        List<Dml> dmlList = new ArrayList<>();
        for (Dml dml : dmls) {
            String destination = StringUtils.trimToEmpty(dml.getDestination());
            String database = dml.getDatabase();
            MirrorDbConfig mirrorDbConfig = mirrorDbConfigCache.get(destination + "." + database);
            if (mirrorDbConfig == null) {
                continue;
            }
            if (mirrorDbConfig.getMappingConfig() == null) {
                continue;
            }
            if (dml.getGroupId() != null && StringUtils.isNotEmpty(mirrorDbConfig.getMappingConfig().getGroupId())) {
                if (!mirrorDbConfig.getMappingConfig().getGroupId().equals(dml.getGroupId())) {
                    continue; // 如果groupId不匹配则过滤
                }
            }

            if (dml.getIsDdl() != null && dml.getIsDdl() && StringUtils.isNotEmpty(dml.getSql())) {
                // DDL
                if (logger.isDebugEnabled()) {
                    logger.debug("DDL: {}", JSON.toJSONString(dml, SerializerFeature.WriteMapNullValue));
                }
                executeDdl(mirrorDbConfig, dml);
                rdbSyncService.getColumnsTypeCache().remove(destination + "." + database + "." + dml.getTable());
                mirrorDbConfig.getTableConfig().remove(dml.getTable()); // 删除对应库表配置
            } else {
                // DML
                initMappingConfig(dml.getTable(), mirrorDbConfig.getMappingConfig(), mirrorDbConfig, dml);
                dmlList.add(dml);
            }
        }
        if (!dmlList.isEmpty()) {
            rdbSyncService.sync(dmlList, dml -> {
                MirrorDbConfig mirrorDbConfig = mirrorDbConfigCache.get(dml.getDestination() + "." + dml.getDatabase());
                if (mirrorDbConfig == null) {
                    return false;
                }
                String table = dml.getTable();
                MappingConfig config = mirrorDbConfig.getTableConfig().get(table);

                if (config == null) {
                    return false;
                }
                // 是否区分大小写
                boolean caseInsensitive = config.getDbMapping().isCaseInsensitive();
                if (config.getConcurrent()) {
                    List<SingleDml> singleDmls = SingleDml.dml2SingleDmls(dml, caseInsensitive);
                    singleDmls.forEach(singleDml -> {
                        int hash = rdbSyncService.pkHash(config.getDbMapping(), singleDml.getData());
                        RdbSyncService.SyncItem syncItem = new RdbSyncService.SyncItem(config, singleDml);
                        rdbSyncService.getDmlsPartition()[hash].add(syncItem);
                    });
                } else {
                    int hash = 0;
                    List<SingleDml> singleDmls = SingleDml.dml2SingleDmls(dml, caseInsensitive);
                    singleDmls.forEach(singleDml -> {
                        RdbSyncService.SyncItem syncItem = new RdbSyncService.SyncItem(config, singleDml);
                        rdbSyncService.getDmlsPartition()[hash].add(syncItem);
                    });
                }
                return true;
            });
        }
    }

    /**
     * 初始化表配置
     *
     * @param key 配置key: destination.database.table
     * @param baseConfigMap db sync config
     * @param dml DML
     */
    private void initMappingConfig(String key, MappingConfig baseConfigMap, MirrorDbConfig mirrorDbConfig, Dml dml) {
        MappingConfig mappingConfig = mirrorDbConfig.getTableConfig().get(key);
        if (mappingConfig == null) {
            // 构造表配置
            mappingConfig = new MappingConfig();
            mappingConfig.setDataSourceKey(baseConfigMap.getDataSourceKey());
            mappingConfig.setDestination(baseConfigMap.getDestination());
            mappingConfig.setGroupId(baseConfigMap.getGroupId());
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

            mirrorDbConfig.getTableConfig().put(key, mappingConfig);
        }
    }

    /**
     * DDL 操作
     *
     * @param ddl DDL
     */
    private void executeDdl(MirrorDbConfig mirrorDbConfig, Dml ddl) {
        try (Connection conn = dataSource.getConnection(); Statement statement = conn.createStatement()) {
            statement.execute(ddl.getSql());
            // 移除对应配置
            mirrorDbConfig.getTableConfig().remove(ddl.getTable());
            if (logger.isTraceEnabled()) {
                logger.trace("Execute DDL sql: {} for database: {}", ddl.getSql(), ddl.getDatabase());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
