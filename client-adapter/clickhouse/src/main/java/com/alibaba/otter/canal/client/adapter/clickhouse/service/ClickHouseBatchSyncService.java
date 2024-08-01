package com.alibaba.otter.canal.client.adapter.clickhouse.service;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter.Feature;
import com.alibaba.otter.canal.client.adapter.clickhouse.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.clickhouse.config.MappingConfig.DbMapping;
import com.alibaba.otter.canal.client.adapter.clickhouse.support.BatchExecutor;
import com.alibaba.otter.canal.client.adapter.clickhouse.support.SingleDml;
import com.alibaba.otter.canal.client.adapter.clickhouse.support.SyncUtil;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.Util;

/**
 * ClickHouse batch synchronize
 *
 * @author: Xander
 * @date: Created in 2023/11/10 22:23
 * @email: zhrunxin33@gmail.com
 * @version 1.1.8
 */
public class ClickHouseBatchSyncService {

    private static final Logger                         logger  = LoggerFactory.getLogger(ClickHouseBatchSyncService.class);

    private DruidDataSource                             dataSource;

    private Map<String, Map<String, Integer>>           columnsTypeCache;     // Cache of instance.schema.table -> <columnName, jdbcType>

    private Map<MappingConfig, List<SingleDml>>[]       bufferPools;          // Data buffer pool store sync data, List<Dml> dispersed as arrays according to hash value

    private BatchExecutor[]                             batchExecutors;       // Batch Executor

    private BatchExecutor                               alterExecutors;       // Alter Single Executor(update/delete/truncate)

    private ExecutorService[]                           executorThreads;      // Be initialized once

    private ScheduledExecutorService[]                  scheduledExecutors;

    private int                                         threads = 3;          // Default parallel thread count
    private int                                         batchSize = 1000;
    private long                                        scheduleTime = 10;
    private boolean                                     skipDupException;

    public Map<String, Map<String, Integer>> getColumnsTypeCache() {
        return columnsTypeCache;
    }

    public ClickHouseBatchSyncService(DruidDataSource dataSource, Integer threads, Integer batchSize, Long scheduleTime, boolean skipDupException){
        this(dataSource, threads, batchSize, scheduleTime, new ConcurrentHashMap<>(), skipDupException);
    }

    @SuppressWarnings("unchecked")
    public ClickHouseBatchSyncService(DruidDataSource dataSource, Integer threads, Integer batchSize, Long scheduleTime, Map<String, Map<String, Integer>> columnsTypeCache,
                                      boolean skipDupException){
        this.dataSource = dataSource;
        this.columnsTypeCache = columnsTypeCache;
        this.skipDupException = skipDupException;
        try {
            if (threads != null) {
                this.threads = threads;
            }
            if (batchSize != null) {
                this.batchSize = batchSize;
            }
            if (scheduleTime != null) {
                this.scheduleTime = scheduleTime;
            }
            this.alterExecutors = new BatchExecutor(dataSource);
            this.bufferPools = new ConcurrentHashMap[this.threads];
            this.batchExecutors = new BatchExecutor[this.threads];
            this.executorThreads = new ExecutorService[this.threads];
            this.scheduledExecutors = new ScheduledExecutorService[this.threads];
            for (int i = 0; i < this.threads; i++) {
                bufferPools[i] = new ConcurrentHashMap<>();
                batchExecutors[i] = new BatchExecutor(dataSource);
                executorThreads[i] = Executors.newSingleThreadExecutor();
                scheduledExecutors[i] = Executors.newSingleThreadScheduledExecutor();
            }
            scheduleBatchSync();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Timing-driven event
     * start schedule batch sync threadPool
     */
    private void scheduleBatchSync() {
        for (int i = 0; i < scheduledExecutors.length; i++) {
            int index = i;
            scheduledExecutors[i].scheduleAtFixedRate(()->{
                List<Future<Boolean>> futures = new ArrayList<>();
                for (MappingConfig mappingConfig : bufferPools[index].keySet()) {
                    List<SingleDml> dmls = bufferPools[index].get(mappingConfig);
                    if (dmls == null || dmls.isEmpty()) {
                        return;
                    }
                    List<SingleDml> tempDmls;
                    synchronized (dmls) {
                        tempDmls = new ArrayList<>(dmls);
                        dmls.clear();
                    }
                    futures.add(executorThreads[index].submit(()->{
                        try {
                            insert(batchExecutors[index], mappingConfig, tempDmls);
                            batchExecutors[index].commit();
                            return true;
                        } catch (Exception e){
                            batchExecutors[index].rollback();
                            throw new RuntimeException(e);
                        }
                    }));
                }
            }, 0, scheduleTime, TimeUnit.SECONDS);
        }
        logger.info("Schedule batch executors has started successfully!");
    }

    /**
     * 批量同步回调
     *
     * @param dmls 批量 DML
     * @param function 回调方法
     */
    public void sync(List<Dml> dmls, Function<Dml, Boolean> function) {
        boolean toExecute = false;
        for (Dml dml : dmls) {
            if (!toExecute) {
                toExecute = function.apply(dml);
            } else {
                function.apply(dml);
            }
        }
    }

    /**
     * Distribute dmls into different partition
     *
     * @param mappingConfig {@link com.alibaba.otter.canal.client.adapter.clickhouse.ClickHouseAdapter#mappingConfigCache }
     * @param dmls received DML
     */
    public void sync(Map<String, Map<String, MappingConfig>> mappingConfig, List<Dml> dmls, Properties envProperties) {
        sync(dmls, dml -> {
            if (dml.getIsDdl() != null && dml.getIsDdl() && StringUtils.isNotEmpty(dml.getSql())) {
                // DDL(Cache need to update when DDL was executed)
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
                distributeDml(config, dml);
            }
            return true;
        }
    }   );
    }

    /**
     * Dml distributor
     */
    private void distributeDml(MappingConfig config, Dml dml) {
        if (config != null) {
            try {
                String type = dml.getType();
                if (type == null) return;

                if (type.equalsIgnoreCase("INSERT")) {
                    appendDmlBufferPartition(config, dml);
                } else {
                    boolean caseInsensitive = config.getDbMapping().isCaseInsensitive();
                    List<SingleDml> singleDmls = SingleDml.dml2SingleDmls(dml, caseInsensitive);

                    if (type.equalsIgnoreCase("UPDATE")) {
                        for (SingleDml singleDml : singleDmls) {
                            update(alterExecutors, config, singleDml);
                        }
                    } else if (type.equalsIgnoreCase("DELETE")) {
                        for (SingleDml singleDml : singleDmls) {
                            delete(alterExecutors, config, singleDml);
                        }
                    } else if (type.equalsIgnoreCase("TRUNCATE")) {
                        truncate(alterExecutors, config);
                    }
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("DML: {}", JSON.toJSONString(dml, Feature.WriteNulls));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void appendDmlBufferPartition(MappingConfig config, Dml dml) {
        boolean caseInsensitive = config.getDbMapping().isCaseInsensitive();
        List<SingleDml> singleDmls = SingleDml.dml2SingleDmls(dml, caseInsensitive);

        singleDmls.forEach(singleDml -> {
            int hash = mappingHash(config.getDbMapping());
            if (!config.getConcurrent()) {
                hash = 0;
            }
            List<SingleDml> dmls = bufferPools[hash].computeIfAbsent(config, k -> new ArrayList<>());
            synchronized (dmls) {
                dmls.add(singleDml);
                logger.info("Append one data into pool, id {}", singleDml.getData().get("id"));
            }
            // Check the size of the List, achieve when it reaches the maximum value
            if (dmls.size() >= batchSize) syncToClickHouse(config, hash);
        });
    }

    /**
     * sync when size of list{@link #bufferPools } reaches the maximum value
     *
     * @param config key
     * @param index parallel thread index
     */
    private void syncToClickHouse(MappingConfig config, int index) {
        List<SingleDml> dmls = bufferPools[index].get(config);
        logger.info("schema:{} table:{} reaches the maximum value, ready to synchronize, size {}", config.getDbMapping().getDatabase(), config.getDbMapping().getTable(), dmls.size());
        if (dmls ==null || dmls.isEmpty()) {
            return;
        }
        List<SingleDml> tempDmls;
        synchronized (dmls) {
            tempDmls = new ArrayList<>(dmls);
            dmls.clear();
        }
        executorThreads[index].submit(() -> {
            try {
                insert(batchExecutors[index], config, tempDmls);
                batchExecutors[index].commit();
                return true;
            } catch (Exception e) {
                batchExecutors[index].rollback();
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Insert
     *
     * @param batchExecutor batch translational executor
     * @param config corresponding configuration object
     * @param dmls DMLs
     */
    private void insert(BatchExecutor batchExecutor, MappingConfig config, List<SingleDml> dmls) throws SQLException {
        if (dmls == null || dmls.isEmpty()) {
            return;
        }
        List<SingleDml> clearDmls = dmls.stream().filter(e -> e.getData() != null && !e.getData().isEmpty()).collect(Collectors.toList());
        if (clearDmls == null || clearDmls.isEmpty()) {
            return;
        }

        DbMapping dbMapping = config.getDbMapping();
        String backtick = SyncUtil.getBacktickByDbType(dataSource.getDbType());
        Map<String, String> columnsMap = SyncUtil.getColumnsMap(dbMapping, clearDmls.get(0).getData());

        StringBuilder insertSql = new StringBuilder();
        insertSql.append("INSERT INTO ").append(SyncUtil.getDbTableName(dbMapping, dataSource.getDbType())).append(" (");

        columnsMap.forEach((targetColumnName, srcColumnName) -> insertSql.append(backtick)
            .append(targetColumnName)
            .append(backtick)
            .append(","));
        int len = insertSql.length();
        insertSql.delete(len - 1, len).append(") VALUES (");
        int mapLen = columnsMap.size();
        for (int i = 0; i < mapLen; i++) {
            insertSql.append("?,");
        }
        len = insertSql.length();
        insertSql.delete(len - 1, len).append(")");

        Map<String, Integer> ctype = getTargetColumnType(batchExecutor.getConn(), config);

        List<List<Map<String, ?>>> values = new ArrayList<>();
        boolean flag = false;
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
            for (int i = 0; i < clearDmls.size(); i++) {
                Map<String, Object> dmlData = clearDmls.get(i).getData();
                List<Map<String, ?>> item;
                if (flag == false) {
                    item = new ArrayList<>();
                    values.add(item);
                } else {
                    item = values.get(i);
                }
                Object value = dmlData.get(srcColumnName);
                BatchExecutor.setValue(item, type, value);
            }
            flag = true;
        }

        try {
            batchExecutor.batchExecute(insertSql.toString(), values);
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
        String backtick = SyncUtil.getBacktickByDbType(dataSource.getDbType());
        Map<String, String> columnsMap = SyncUtil.getColumnsMap(dbMapping, data);

        Map<String, Integer> ctype = getTargetColumnType(batchExecutor.getConn(), config);

        StringBuilder updateSql = new StringBuilder();
        updateSql.append("ALTER TABLE ").append(SyncUtil.getDbTableName(dbMapping, dataSource.getDbType())).append(" UPDATE ");
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
                    updateSql.append(backtick).append(targetColumnName).append(backtick).append("=?, ");
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
            logger.trace("Alter update target table, sql: {}", updateSql);
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
        sql.append("ALTER TABLE ").append(SyncUtil.getDbTableName(dbMapping, dataSource.getDbType())).append(" DELETE WHERE ");

        List<Map<String, ?>> values = new ArrayList<>();
        // 拼接主键
        appendCondition(dbMapping, sql, ctype, values, data);
        batchExecutor.execute(sql.toString(), values);
        if (logger.isTraceEnabled()) {
            logger.trace("Alter delete from target table, sql: {}", sql);
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
        sql.append("TRUNCATE TABLE ").append(SyncUtil.getDbTableName(dbMapping, dataSource.getDbType()));
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
            synchronized (ClickHouseBatchSyncService.class) {
                columnType = columnsTypeCache.get(cacheKey);
                if (columnType == null) {
                    columnType = new LinkedHashMap<>();
                    final Map<String, Integer> columnTypeTmp = columnType;
                    String sql = "SELECT * FROM " + SyncUtil.getDbTableName(dbMapping, dataSource.getDbType()) + " WHERE 1=2";
                    Util.sqlRS(conn, sql, rs -> {
                        try {
                            ResultSetMetaData rsd = rs.getMetaData();
                            int columnCount = rsd.getColumnCount();
                            for (int i = 1; i <= columnCount; i++) {
                                int colType = rsd.getColumnType(i);
                                // 修复year类型作为date处理时的data truncated问题
                                if ("YEAR".equals(rsd.getColumnTypeName(i))) {
                                    colType = Types.VARCHAR;
                                }
                                columnTypeTmp.put(rsd.getColumnName(i).toLowerCase(), colType);
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
        String backtick = SyncUtil.getBacktickByDbType(dataSource.getDbType());

        // 拼接主键
        for (Map.Entry<String, String> entry : dbMapping.getTargetPk().entrySet()) {
            String targetColumnName = entry.getKey();
            String srcColumnName = entry.getValue();
            if (srcColumnName == null) {
                srcColumnName = Util.cleanColumn(targetColumnName);
            }
            sql.append(backtick).append(targetColumnName).append(backtick).append("=? AND ");
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

    /**
     * make sure the same table in one index
     *
     * @param dbMapping
     * @return
     */
    private int mappingHash(MappingConfig.DbMapping dbMapping) {
        int hash = dbMapping.getDatabase().toLowerCase().hashCode() + dbMapping.getTable().toLowerCase().hashCode();
        hash = Math.abs(hash) % threads;
        return Math.abs(hash);
    }

    public void close() {
        for (int i = 0; i < threads; i++) {
            executorThreads[i].shutdown();
        }
    }
}
