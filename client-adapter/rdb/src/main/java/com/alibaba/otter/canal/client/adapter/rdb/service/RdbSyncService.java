package com.alibaba.otter.canal.client.adapter.rdb.service;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

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

    private static final Logger               logger  = LoggerFactory.getLogger(RdbSyncService.class);

    // 源库表字段类型缓存: instance.schema.table -> <columnName, jdbcType>
    private Map<String, Map<String, Integer>> columnsTypeCache;

    private int                               threads = 3;
    private boolean                           skipDupException;

    private List<SyncItem>[]                  dmlsPartition;
    private BatchExecutor[]                   batchExecutors;
    private ExecutorService[]                 executorThreads;

    public List<SyncItem>[] getDmlsPartition() {
        return dmlsPartition;
    }

    public Map<String, Map<String, Integer>> getColumnsTypeCache() {
        return columnsTypeCache;
    }

    public RdbSyncService(DataSource dataSource, Integer threads, boolean skipDupException){
        this(dataSource, threads, new ConcurrentHashMap<>(), skipDupException);
    }

    @SuppressWarnings("unchecked")
    public RdbSyncService(DataSource dataSource, Integer threads, Map<String, Map<String, Integer>> columnsTypeCache,
                          boolean skipDupException){
        this.columnsTypeCache = columnsTypeCache;
        this.skipDupException = skipDupException;
        try {
            if (threads != null) {
                this.threads = threads;
            }
            this.dmlsPartition = new List[this.threads];
            this.batchExecutors = new BatchExecutor[this.threads];
            this.executorThreads = new ExecutorService[this.threads];
            for (int i = 0; i < this.threads; i++) {
                dmlsPartition[i] = new ArrayList<>();
                batchExecutors[i] = new BatchExecutor(dataSource);
                executorThreads[i] = Executors.newSingleThreadExecutor();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 批量同步回调
     *
     * @param dmls 批量 DML
     * @param function 回调方法
     */
    public void sync(List<Dml> dmls, Function<Dml, Boolean> function) {
        try {
            boolean toExecute = false;
            for (Dml dml : dmls) {
                if (!toExecute) {
                    toExecute = function.apply(dml);
                } else {
                    function.apply(dml);
                }
            }
            if (toExecute) {
                List<Future<Boolean>> futures = new ArrayList<>();
                for (int i = 0; i < threads; i++) {
                    int j = i;
                    if (dmlsPartition[j].isEmpty()) {
                        // bypass
                        continue;
                    }

                    futures.add(executorThreads[i].submit(() -> {
                        try {
                            dmlsPartition[j].forEach(syncItem -> sync(batchExecutors[j],
                                syncItem.config,
                                syncItem.singleDml));
                            dmlsPartition[j].clear();
                            batchExecutors[j].commit();
                            return true;
                        } catch (Throwable e) {
                            batchExecutors[j].rollback();
                            throw new RuntimeException(e);
                        }
                    }));
                }

                futures.forEach(future -> {
                    try {
                        future.get();
                    } catch (ExecutionException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        } finally {
            for (BatchExecutor batchExecutor : batchExecutors) {
                if (batchExecutor != null) {
                    batchExecutor.close();
                }
            }
        }
    }

    /**
     * 批量同步
     *
     * @param mappingConfig 配置集合
     * @param dmls 批量 DML
     */
    public void sync(Map<String, Map<String, MappingConfig>> mappingConfig, List<Dml> dmls, Properties envProperties) {
        sync(dmls, dml -> {
            if (dml.getIsDdl() != null && dml.getIsDdl() && StringUtils.isNotEmpty(dml.getSql())) {
                // DDL
            columnsTypeCache.remove(dml.getDestination() + "." + dml.getDatabase() + "." + dml.getTable());
            return false;
        } else {
            // DML
            String destination = StringUtils.trimToEmpty(dml.getDestination());
            String groupId = StringUtils.trimToEmpty(dml.getGroupId());
            String database = dml.getDatabase();
            String table = dml.getTable();
            Map<String, MappingConfig> configMap;
            if (envProperties != null && !"tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
                configMap = mappingConfig.get(destination + "-" + groupId + "_" + database + "-" + table);
            } else {
                configMap = mappingConfig.get(destination + "_" + database + "-" + table);
            }

            if (configMap == null) {
                return false;
            }

            if (configMap.values().isEmpty()) {
                return false;
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
                    int hash = 0;
                    List<SingleDml> singleDmls = SingleDml.dml2SingleDmls(dml);
                    singleDmls.forEach(singleDml -> {
                        SyncItem syncItem = new SyncItem(config, singleDml);
                        dmlsPartition[hash].add(syncItem);
                    });
                }
            }
            return true;
        }
    }   );
    }

    /**
     * 单条 dml 同步
     *
     * @param batchExecutor 批量事务执行器
     * @param config 对应配置对象
     * @param dml DML
     */
    public void sync(BatchExecutor batchExecutor, MappingConfig config, SingleDml dml) {
        if (config != null) {
            try {
                String type = dml.getType();
                if (type != null && type.equalsIgnoreCase("INSERT")) {
                    insert(batchExecutor, config, dml);
                } else if (type != null && type.equalsIgnoreCase("UPDATE")) {
                    update(batchExecutor, config, dml);
                } else if (type != null && type.equalsIgnoreCase("DELETE")) {
                    delete(batchExecutor, config, dml);
                } else if (type != null && type.equalsIgnoreCase("TRUNCATE")) {
                    truncate(batchExecutor, config);
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("DML: {}", JSON.toJSONString(dml, SerializerFeature.WriteMapNullValue));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 插入操作
     *
     * @param config 配置项
     * @param dml DML数据
     */
    private void insert(BatchExecutor batchExecutor, MappingConfig config, SingleDml dml) throws SQLException {
        Map<String, Object> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }

        DbMapping dbMapping = config.getDbMapping();

        Map<String, String> columnsMap = SyncUtil.getColumnsMap(dbMapping, data);

        StringBuilder insertSql = new StringBuilder();
        insertSql.append("INSERT INTO ").append(SyncUtil.getDbTableName(dbMapping)).append(" (");

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
            if (type == null) {
                throw new RuntimeException("Target column: " + targetColumnName + " not matched");
            }
            Object value = data.get(srcColumnName);
            BatchExecutor.setValue(values, type, value);
        }

        try {
            batchExecutor.execute(insertSql.toString(), values);
        } catch (SQLException e) {
            if (skipDupException
                && (e.getMessage().contains("Duplicate entry") || e.getMessage().startsWith("ORA-00001:"))) {
                // ignore
                // TODO 增加更多关系数据库的主键冲突的错误码
            } else {
                throw e;
            }
        }
        if (logger.isTraceEnabled()) {
            logger.trace("Insert into target table, sql: {}", insertSql);
        }

    }

    /**
     * 更新操作
     *
     * @param config 配置项
     * @param dml DML数据
     */
    private void update(BatchExecutor batchExecutor, MappingConfig config, SingleDml dml) throws SQLException {
        Map<String, Object> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }

        Map<String, Object> old = dml.getOld();
        if (old == null || old.isEmpty()) {
            return;
        }

        DbMapping dbMapping = config.getDbMapping();

        Map<String, String> columnsMap = SyncUtil.getColumnsMap(dbMapping, data);

        Map<String, Integer> ctype = getTargetColumnType(batchExecutor.getConn(), config);

        StringBuilder updateSql = new StringBuilder();
        updateSql.append("UPDATE ").append(SyncUtil.getDbTableName(dbMapping)).append(" SET ");
        List<Map<String, ?>> values = new ArrayList<>();
        boolean hasMatched = false;
        for (String srcColumnName : old.keySet()) {
            List<String> targetColumnNames = new ArrayList<>();
            columnsMap.forEach((targetColumn, srcColumn) -> {
                if (srcColumnName.equalsIgnoreCase(srcColumn)) {
                    targetColumnNames.add(targetColumn);
                }
            });
            if (!targetColumnNames.isEmpty()) {
                hasMatched = true;
                for (String targetColumnName : targetColumnNames) {
                    updateSql.append(targetColumnName).append("=?, ");
                    Integer type = ctype.get(Util.cleanColumn(targetColumnName).toLowerCase());
                    if (type == null) {
                        throw new RuntimeException("Target column: " + targetColumnName + " not matched");
                    }
                    BatchExecutor.setValue(values, type, data.get(srcColumnName));
                }
            }
        }
        if (!hasMatched) {
            logger.warn("Did not matched any columns to update ");
            return;
        }
        int len = updateSql.length();
        updateSql.delete(len - 2, len).append(" WHERE ");

        // 拼接主键
        appendCondition(dbMapping, updateSql, ctype, values, data, old);
        batchExecutor.execute(updateSql.toString(), values);
        if (logger.isTraceEnabled()) {
            logger.trace("Update target table, sql: {}", updateSql);
        }
    }

    /**
     * 删除操作
     *
     * @param config
     * @param dml
     */
    private void delete(BatchExecutor batchExecutor, MappingConfig config, SingleDml dml) throws SQLException {
        Map<String, Object> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }

        DbMapping dbMapping = config.getDbMapping();

        Map<String, Integer> ctype = getTargetColumnType(batchExecutor.getConn(), config);

        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ").append(SyncUtil.getDbTableName(dbMapping)).append(" WHERE ");

        List<Map<String, ?>> values = new ArrayList<>();
        // 拼接主键
        appendCondition(dbMapping, sql, ctype, values, data);
        batchExecutor.execute(sql.toString(), values);
        if (logger.isTraceEnabled()) {
            logger.trace("Delete from target table, sql: {}", sql);
        }
    }

    /**
     * truncate操作
     *
     * @param config
     */
    private void truncate(BatchExecutor batchExecutor, MappingConfig config) throws SQLException {
        DbMapping dbMapping = config.getDbMapping();
        StringBuilder sql = new StringBuilder();
        sql.append("TRUNCATE TABLE ").append(SyncUtil.getDbTableName(dbMapping));
        batchExecutor.execute(sql.toString(), new ArrayList<>());
        if (logger.isTraceEnabled()) {
            logger.trace("Truncate target table, sql: {}", sql);
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
        Map<String, Integer> columnType = columnsTypeCache.get(cacheKey);
        if (columnType == null) {
            synchronized (RdbSyncService.class) {
                columnType = columnsTypeCache.get(cacheKey);
                if (columnType == null) {
                    columnType = new LinkedHashMap<>();
                    final Map<String, Integer> columnTypeTmp = columnType;
                    String sql = "SELECT * FROM " + SyncUtil.getDbTableName(dbMapping) + " WHERE 1=2";
                    Util.sqlRS(conn, sql, rs -> {
                        try {
                            ResultSetMetaData rsd = rs.getMetaData();
                            int columnCount = rsd.getColumnCount();
                            for (int i = 1; i <= columnCount; i++) {
                                columnTypeTmp.put(rsd.getColumnName(i).toLowerCase(), rsd.getColumnType(i));
                            }
                            columnsTypeCache.put(cacheKey, columnTypeTmp);
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
            if (type == null) {
                throw new RuntimeException("Target column: " + targetColumnName + " not matched");
            }
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

    public static class SyncItem {

        private MappingConfig config;
        private SingleDml     singleDml;

        public SyncItem(MappingConfig config, SingleDml singleDml){
            this.config = config;
            this.singleDml = singleDml;
        }
    }

    /**
     * 取主键hash
     */
    public int pkHash(DbMapping dbMapping, Map<String, Object> d) {
        return pkHash(dbMapping, d, null);
    }

    public int pkHash(DbMapping dbMapping, Map<String, Object> d, Map<String, Object> o) {
        int hash = 0;
        // 取主键
        for (Map.Entry<String, String> entry : dbMapping.getTargetPk().entrySet()) {
            String targetColumnName = entry.getKey();
            String srcColumnName = entry.getValue();
            if (srcColumnName == null) {
                srcColumnName = Util.cleanColumn(targetColumnName);
            }
            Object value = null;
            if (o != null && o.containsKey(srcColumnName)) {
                value = o.get(srcColumnName);
            } else if (d != null) {
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
            executorThreads[i].shutdown();
        }
    }
}
