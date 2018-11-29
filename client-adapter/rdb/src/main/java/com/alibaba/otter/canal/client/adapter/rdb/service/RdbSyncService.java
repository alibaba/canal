package com.alibaba.otter.canal.client.adapter.rdb.service;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfig.DbMapping;
import com.alibaba.otter.canal.client.adapter.rdb.support.BatchExecutor;
import com.alibaba.otter.canal.client.adapter.rdb.support.SingleDml;
import com.alibaba.otter.canal.client.adapter.rdb.support.SyncUtil;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.Util;

/**
 * RDB同步操作业务
 *
 * @author rewerma 2018-11-7 下午06:45:49
 * @version 1.0.0
 */
public class RdbSyncService {

    private static final Logger                     logger             = LoggerFactory.getLogger(RdbSyncService.class);

    private final Map<String, Map<String, Integer>> COLUMNS_TYPE_CACHE = new ConcurrentHashMap<>();

    private Map<String, Map<String, MappingConfig>> mappingConfigCache;                                                // 库名-表名对应配置

    private int                                     threads            = 3;

    private List<SyncItem>[]                        dmlsPartition;
    private BatchExecutor[]                         batchExecutors;
    private ExecutorService[]                       executorThreads;

    @SuppressWarnings("unchecked")
    public RdbSyncService(Map<String, Map<String, MappingConfig>> mappingConfigCache, DataSource dataSource,
                          Integer threads){
        try {
            if (threads != null) {
                this.threads = threads;
            }
            this.mappingConfigCache = mappingConfigCache;
            this.dmlsPartition = new List[this.threads];
            this.batchExecutors = new BatchExecutor[this.threads];
            this.executorThreads = new ExecutorService[this.threads];
            for (int i = 0; i < this.threads; i++) {
                dmlsPartition[i] = new ArrayList<>();
                batchExecutors[i] = new BatchExecutor(dataSource.getConnection());
                executorThreads[i] = Executors.newSingleThreadExecutor();
            }
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void sync(List<Dml> dmls) {
        try {
            for (Dml dml : dmls) {
                String destination = StringUtils.trimToEmpty(dml.getDestination());
                String database = dml.getDatabase();
                String table = dml.getTable();
                Map<String, MappingConfig> configMap = mappingConfigCache
                    .get(destination + "." + database + "." + table);

                if (configMap == null) {
                    continue;
                }
                for (MappingConfig config : configMap.values()) {

                    if (config.getConcurrent()) {
                        List<SingleDml> singleDmls = SingleDml.dml2SingleDmls(dml);
                        singleDmls.forEach(singleDml -> {
                            int hash = pkHash(config.getDbMapping(), singleDml.getData());
                            SyncItem syncItem = new SyncItem(config, singleDml);
                            dmlsPartition[hash].add(syncItem);
                        });
                    } else {
                        int hash = Math.abs(Math.abs(config.getDbMapping().getTargetTable().hashCode()) % threads);
                        List<SingleDml> singleDmls = SingleDml.dml2SingleDmls(dml);
                        singleDmls.forEach(singleDml -> {
                            SyncItem syncItem = new SyncItem(config, singleDml);
                            dmlsPartition[hash].add(syncItem);
                        });
                    }
                }
            }
            List<Future> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                int j = i;
                futures.add(executorThreads[i].submit(() -> {
                    dmlsPartition[j].forEach(syncItem -> sync(batchExecutors[j], syncItem.config, syncItem.singleDml));
                    batchExecutors[j].commit();
                    return true;
                }));
            }

            futures.forEach(future -> {
                try {
                    future.get();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            });

            for (int i = 0; i < threads; i++) {
                dmlsPartition[i].clear();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void sync(BatchExecutor batchExecutor, MappingConfig config, SingleDml dml) {
        try {
            if (config != null) {
                String type = dml.getType();
                if (type != null && type.equalsIgnoreCase("INSERT")) {
                    insert(batchExecutor, config, dml);
                } else if (type != null && type.equalsIgnoreCase("UPDATE")) {
                    update(batchExecutor, config, dml);
                } else if (type != null && type.equalsIgnoreCase("DELETE")) {
                    delete(batchExecutor, config, dml);
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("DML: {}", JSON.toJSONString(dml, SerializerFeature.WriteMapNullValue));
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 插入操作
     *
     * @param config 配置项
     * @param dml DML数据
     */
    private void insert(BatchExecutor batchExecutor, MappingConfig config, SingleDml dml) {
        Map<String, Object> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }

        DbMapping dbMapping = config.getDbMapping();

        try {
            Map<String, String> columnsMap = SyncUtil.getColumnsMap(dbMapping, data);

            StringBuilder insertSql = new StringBuilder();
            insertSql.append("INSERT INTO ").append(dbMapping.getTargetTable()).append(" (");

            columnsMap.forEach((targetColumnName, srcColumnName) -> insertSql.append(targetColumnName).append(","));
            int len = insertSql.length();
            insertSql.delete(len - 1, len).append(") VALUES (");
            int mapLen = columnsMap.size();
            for (int i = 0; i < mapLen; i++) {
                insertSql.append("?,");
            }
            len = insertSql.length();
            insertSql.delete(len - 1, len).append(")");

            Map<String, Integer> ctype = getTargetColumnType(batchExecutor.getConn(), config);

            List<Map<String, ?>> values = new ArrayList<>();
            for (Map.Entry<String, String> entry : columnsMap.entrySet()) {
                String targetColumnName = entry.getKey();
                String srcColumnName = entry.getValue();
                if (srcColumnName == null) {
                    srcColumnName = Util.cleanColumn(targetColumnName);
                }

                Integer type = ctype.get(Util.cleanColumn(targetColumnName).toLowerCase());

                Object value = data.get(srcColumnName);

                BatchExecutor.setValue(values, type, value);
            }

            batchExecutor.execute(insertSql.toString(), values);
            if (logger.isTraceEnabled()) {
                logger.trace("Insert into target table, sql: {}", insertSql);
            }

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 更新操作
     *
     * @param config 配置项
     * @param dml DML数据
     */
    private void update(BatchExecutor batchExecutor, MappingConfig config, SingleDml dml) {
        Map<String, Object> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }

        Map<String, Object> old = dml.getOld();
        if (old == null || old.isEmpty()) {
            return;
        }

        DbMapping dbMapping = config.getDbMapping();

        try {
            Map<String, String> columnsMap = SyncUtil.getColumnsMap(dbMapping, data);

            Map<String, Integer> ctype = getTargetColumnType(batchExecutor.getConn(), config);

            StringBuilder updateSql = new StringBuilder();
            updateSql.append("UPDATE ").append(dbMapping.getTargetTable()).append(" SET ");
            List<Map<String, ?>> values = new ArrayList<>();
            for (String srcColumnName : old.keySet()) {
                List<String> targetColumnNames = new ArrayList<>();
                columnsMap.forEach((targetColumn, srcColumn) -> {
                    if (srcColumnName.toLowerCase().equals(srcColumn)) {
                        targetColumnNames.add(targetColumn);
                    }
                });
                if (!targetColumnNames.isEmpty()) {

                    for (String targetColumnName : targetColumnNames) {
                        updateSql.append(targetColumnName).append("=?, ");
                        Integer type = ctype.get(Util.cleanColumn(targetColumnName).toLowerCase());
                        BatchExecutor.setValue(values, type, data.get(srcColumnName));
                    }
                }
            }
            int len = updateSql.length();
            updateSql.delete(len - 2, len).append(" WHERE ");

            // 拼接主键
            appendCondition(dbMapping, updateSql, ctype, values, data, old);

            batchExecutor.execute(updateSql.toString(), values);

            if (logger.isTraceEnabled()) {
                logger.trace("Update target table, sql: {}", updateSql);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 删除操作
     *
     * @param config
     * @param dml
     */
    private void delete(BatchExecutor batchExecutor, MappingConfig config, SingleDml dml) {
        Map<String, Object> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }

        DbMapping dbMapping = config.getDbMapping();

        try {
            Map<String, Integer> ctype = getTargetColumnType(batchExecutor.getConn(), config);

            StringBuilder sql = new StringBuilder();
            sql.append("DELETE FROM ").append(dbMapping.getTargetTable()).append(" WHERE ");

            List<Map<String, ?>> values = new ArrayList<>();
            // 拼接主键
            appendCondition(dbMapping, sql, ctype, values, data);

            batchExecutor.execute(sql.toString(), values);

            if (logger.isTraceEnabled()) {
                logger.trace("Delete from target table, sql: {}", sql);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 获取目标字段类型
     *
     * @param conn sql connection
     * @param config 映射配置
     * @return 字段sqlType
     */
    private Map<String, Integer> getTargetColumnType(Connection conn, MappingConfig config) {
        DbMapping dbMapping = config.getDbMapping();
        String cacheKey = config.getDestination() + "." + dbMapping.getDatabase() + "." + dbMapping.getTable();
        Map<String, Integer> columnType = COLUMNS_TYPE_CACHE.get(cacheKey);
        if (columnType == null) {
            synchronized (RdbSyncService.class) {
                columnType = COLUMNS_TYPE_CACHE.get(cacheKey);
                if (columnType == null) {
                    columnType = new LinkedHashMap<>();
                    final Map<String, Integer> columnTypeTmp = columnType;
                    String sql = "SELECT * FROM " + dbMapping.getTargetTable() + " WHERE 1=2";
                    Util.sqlRS(conn, sql, rs -> {
                        try {
                            ResultSetMetaData rsd = rs.getMetaData();
                            int columnCount = rsd.getColumnCount();
                            for (int i = 1; i <= columnCount; i++) {
                                columnTypeTmp.put(rsd.getColumnName(i).toLowerCase(), rsd.getColumnType(i));
                            }
                            COLUMNS_TYPE_CACHE.put(cacheKey, columnTypeTmp);
                        } catch (SQLException e) {
                            logger.error(e.getMessage(), e);
                        }
                    });
                }
            }
        }
        return columnType;
    }

    /**
     * 拼接主键 where条件
     */
    private void appendCondition(MappingConfig.DbMapping dbMapping, StringBuilder sql, Map<String, Integer> ctype,
                                 List<Map<String, ?>> values, Map<String, Object> d) {
        appendCondition(dbMapping, sql, ctype, values, d, null);
    }

    private void appendCondition(MappingConfig.DbMapping dbMapping, StringBuilder sql, Map<String, Integer> ctype,
                                 List<Map<String, ?>> values, Map<String, Object> d, Map<String, Object> o) {
        // 拼接主键
        for (Map.Entry<String, String> entry : dbMapping.getTargetPk().entrySet()) {
            String targetColumnName = entry.getKey();
            String srcColumnName = entry.getValue();
            if (srcColumnName == null) {
                srcColumnName = Util.cleanColumn(targetColumnName);
            }
            sql.append(targetColumnName).append("=? AND ");
            Integer type = ctype.get(Util.cleanColumn(targetColumnName).toLowerCase());
            // 如果有修改主键的情况
            if (o != null && o.containsKey(srcColumnName)) {
                BatchExecutor.setValue(values, type, o.get(srcColumnName));
            } else {
                BatchExecutor.setValue(values, type, d.get(srcColumnName));
            }
        }
        int len = sql.length();
        sql.delete(len - 4, len);
    }

    private class SyncItem {

        private MappingConfig config;
        private SingleDml     singleDml;

        private SyncItem(MappingConfig config, SingleDml singleDml){
            this.config = config;
            this.singleDml = singleDml;
        }
    }

    /**
     * 取主键hash
     */
    private int pkHash(DbMapping dbMapping, Map<String, Object> d) {
        return pkHash(dbMapping, d, null);
    }

    private int pkHash(DbMapping dbMapping, Map<String, Object> d, Map<String, Object> o) {
        int hash = 0;
        // 取主键
        for (Map.Entry<String, String> entry : dbMapping.getTargetPk().entrySet()) {
            String targetColumnName = entry.getKey();
            String srcColumnName = entry.getValue();
            if (srcColumnName == null) {
                srcColumnName = Util.cleanColumn(targetColumnName);
            }
            Object value;
            if (o != null && o.containsKey(srcColumnName)) {
                value = o.get(srcColumnName);
            } else {
                value = d.get(srcColumnName);
            }
            if (value != null) {
                hash += value.hashCode();
            }
        }
        hash = Math.abs(hash) % threads;
        return Math.abs(hash);
    }

    public void close() {
        for (int i = 0; i < threads; i++) {
            batchExecutors[i].close();
            executorThreads[i].shutdown();
        }
    }
}
