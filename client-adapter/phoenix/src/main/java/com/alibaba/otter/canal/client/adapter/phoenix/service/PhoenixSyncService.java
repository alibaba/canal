package com.alibaba.otter.canal.client.adapter.phoenix.service;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.util.JdbcConstants;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter.Feature;
import com.alibaba.otter.canal.client.adapter.phoenix.config.ConfigurationManager;
import com.alibaba.otter.canal.client.adapter.phoenix.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.phoenix.config.MappingConfig.DbMapping;
import com.alibaba.otter.canal.client.adapter.phoenix.support.BatchExecutor;
import com.alibaba.otter.canal.client.adapter.phoenix.support.SingleDml;
import com.alibaba.otter.canal.client.adapter.phoenix.support.SyncUtil;
import com.alibaba.otter.canal.client.adapter.phoenix.support.TypeUtil;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.Util;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

/**
 * Phoenix同步操作业务
 */
public class PhoenixSyncService {

    private static final Logger logger = LoggerFactory.getLogger(PhoenixSyncService.class);

    // 源库表字段类型缓存: instance.schema.table -> <columnName, jdbcType>
    private Map<String, Map<String, Integer>> columnsTypeCache;

    //同步线程数
    //默认开启3个线程同步,此处配置了自定义同步线程数
    private int threads = ConfigurationManager.getInteger("threads");

    private List<SyncItem>[] dmlsPartition;
    private BatchExecutor[] batchExecutors;
    private ExecutorService[] executorThreads;

    public PhoenixSyncService(Integer threads) {
        this(threads, new ConcurrentHashMap<>());
    }


    @SuppressWarnings("unchecked")
    private PhoenixSyncService(Integer threads, Map<String, Map<String, Integer>> columnsTypeCache) {
        this.columnsTypeCache = columnsTypeCache;
        try {
            if (threads != null) {
                this.threads = threads;
            }
            this.dmlsPartition = new List[this.threads];
            this.batchExecutors = new BatchExecutor[this.threads];
            this.executorThreads = new ExecutorService[this.threads];
            for (int i = 0; i < this.threads; i++) {
                dmlsPartition[i] = new ArrayList<>();
                batchExecutors[i] = new BatchExecutor();
                //创建单个线程，用来操作一个无界的队列任务，不会使用额外的线程。如果线程崩溃会重新创建一个，直到任务完成。
                executorThreads[i] = Executors.newSingleThreadExecutor();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 批量同步回调
     *
     * @param dmls     批量 DML
     * @param function 回调方法
     */
    private void sync(List<Dml> dmls, Function<Dml, Boolean> function) {
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
                            //相对于RDB同步 少了  dmlsPartition[j].clear();
                            //在 try catch中获取异常后再次执行一次batchExecutors[j].commit()
                            batchExecutors[j].commit();
                            return true;
                        } catch (Throwable e) {
                            batchExecutors[j].rollback();
                            if (!e.getClass().getName().endsWith("ColumnNotFoundException")
                                    && !e.getClass().getName().endsWith("TableNotFoundException")) {
                                throw new RuntimeException(e);
                            }
                            logger.info("table or column not found: " + e.getMessage());
                            boolean synced = false;
                            for (SyncItem syncItem : dmlsPartition[j]) {
                                if (PhoenixEtlService.syncSchema(batchExecutors[j].getConn(), syncItem.config)) {
                                    synced = true;
                                }
                            }
                            if (!synced) {
                                throw new RuntimeException(e);
                            }
                            dmlsPartition[j].forEach(syncItem -> sync(batchExecutors[j],
                                    syncItem.config,
                                    syncItem.singleDml));
                            try {
                                batchExecutors[j].commit();
                                return true;
                            } catch (Throwable e1) {
                                batchExecutors[j].rollback();
                                throw new RuntimeException(e1);
                            }
                        } finally {
                            dmlsPartition[j].clear();
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
     * 批量同步 :将批量DML进行解析并放入List<SingleDml> --> dmlsPartition[hash].add(syncItem);
     * @param mappingConfig 配置集合
     * @param dmls   批量 DML
     */
    public void sync(Map<String, Map<String, MappingConfig>> mappingConfig, List<Dml> dmls, Properties envProperties) {
        sync(dmls, dml -> {
            String destination = StringUtils.trimToEmpty(dml.getDestination());
            String groupId = StringUtils.trimToEmpty(dml.getGroupId());
            String database = dml.getDatabase();
            String table = dml.getTable().toLowerCase();
            Map<String, MappingConfig> configMap;
            if (envProperties != null && !"tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
                //tcp 模式
                configMap = mappingConfig.get(destination + "-" + groupId + "_" + database + "-" + table);
            } else {
                //kafka 模式 或者 RocketMQ模式
                configMap = mappingConfig.get(destination + "_" + database + "-" + table);
            }

            if (configMap == null) {
                if (logger.isTraceEnabled()) {
                    logger.trace("no config map: destination={},groupId={}, database={}, table={}, keys={}", destination, groupId, database, table, mappingConfig.keySet());
                }
                return false;
            }
            if (configMap.values().isEmpty()) {
                logger.info("config map empty: destination={},groupId={}, database={}, table={}, keys={}", destination, groupId, database, table, mappingConfig.keySet());
                return false;
            }
            if (dml.getIsDdl() != null && dml.getIsDdl() && StringUtils.isNotEmpty(dml.getSql())) {
                // DDL
                columnsTypeCache.remove(dml.getDestination() + "." + dml.getDatabase() + "." + dml.getTable());
                List<SQLStatement> stmtList;
                try {
                    stmtList = SQLUtils.parseStatements(dml.getSql(), JdbcConstants.MYSQL, false);
                } catch (ParserException e) {
                    // 可能存在一些SQL是不支持的，比如存储过程
                    logger.info("parse sql error: " + dml.getSql(), e);
                    return false;
                }
                for (Map.Entry<String, MappingConfig> entry : configMap.entrySet()) {
                    try {
                        alter(batchExecutors[0], entry.getValue(), dml, stmtList, entry.getKey());
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
                return false;
            } else {
                // DML
                for (Map.Entry<String, MappingConfig> entry : configMap.entrySet()) {
                    MappingConfig config = entry.getValue();
                    if (config.isDebug()) {
                        logger.info("DML: {} {}", entry.getKey(), JSON.toJSONString(dml, Feature.WriteNulls));
                    }
                    if (config.getConcurrent()) {
                        //并行同步
                        //将一批DML转成SingleDml
                        List<SingleDml> singleDmls = SingleDml.dml2SingleDmls(dml);
                        singleDmls.forEach(singleDml -> {
                            //取主键hash
                            int hash = pkHash(config.getDbMapping(), singleDml.getData());
                            SyncItem syncItem = new SyncItem(config, singleDml);
                            //相同的主键数据的顺序是可以保证的
                            dmlsPartition[hash].add(syncItem);
                        });
                    } else {
                        //不并行同步
                        int hash = 0;
                        List<SingleDml> singleDmls = SingleDml.dml2SingleDmls(dml);
                        singleDmls.forEach(singleDml -> {
                            SyncItem syncItem = new SyncItem(config, singleDml);
                            //这里线程默认是3个,如果不并行，则会出现2个线程空跑
                            dmlsPartition[hash].add(syncItem);
                        });
                    }
                }
                return true;
            }
        });
    }

    /**
     * 单条 dml 同步
     *
     * @param batchExecutor 批量事务执行器
     * @param config        对应配置对象
     * @param dml           DML
     */
    private void sync(BatchExecutor batchExecutor, MappingConfig config, SingleDml dml) {
        if (config != null) {
            try {
                String type = dml.getType();
                if (type != null && type.equalsIgnoreCase("INSERT")) {
                    insert(batchExecutor, config, dml);
                } else if (type != null && type.equalsIgnoreCase("UPDATE")) {
                    insert(batchExecutor, config, dml);
                } else if (type != null && type.equalsIgnoreCase("DELETE")) {
                    delete(batchExecutor, config, dml);
                } else if (type != null && type.equalsIgnoreCase("TRUNCATE")) {
                    truncate(batchExecutor, config);
                } else if (logger.isInfoEnabled()){
                    logger.info("SingleDml: {}", JSON.toJSONString(dml, Feature.WriteNulls));
                }
            } catch (SQLException e) {
                logger.error("sync error: " + e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }
    }

    private void alter(BatchExecutor batchExecutor, MappingConfig config, Dml dml, List<SQLStatement> stmtList, String configFile) throws SQLException {
        if (config.isDebug()) {
            logger.info("DML: {} {}", configFile, JSON.toJSONString(dml, Feature.WriteNulls));
        }
        DbMapping dbMapping = config.getDbMapping();
        if (!dbMapping.isAlter()) {
            logger.info("not alterable table: {} {}", dml.getTable(), configFile);
            return;
        }

        Map<String, String> columnsMap = dbMapping.getTargetColumns();

        Map<String, String> columnsMap1 = new HashMap<>();
        for (Map.Entry<String, String> entry : columnsMap.entrySet()) {
            columnsMap1.put(entry.getValue(), entry.getKey());
        }

        String targetTable = SyncUtil.getDbTableName(dbMapping);
        Map<String, String> defValues = new HashMap<>();
        for (SQLStatement statement : stmtList) {
            if (statement instanceof SQLAlterTableStatement) {
                SQLAlterTableStatement alterTable = (SQLAlterTableStatement) statement;
                for (SQLAlterTableItem item : alterTable.getItems()) {
                    if (item instanceof SQLAlterTableDropColumnItem) {
                        SQLAlterTableDropColumnItem dropColumnItem = (SQLAlterTableDropColumnItem) item;
                        if (!dbMapping.isDrop()) {
                            logger.info("drop table column disabled: {} {}", targetTable, dropColumnItem.getColumns());
                            continue;
                        }
                        for (SQLName sqlName : dropColumnItem.getColumns()) {
                            String name = Util.cleanColumn(sqlName.getSimpleName());
                            String sql = "ALTER TABLE " + targetTable + " DROP COLUMN IF EXISTS " +
                                    dbMapping.escape(columnsMap1.getOrDefault(name, name));
                            try {
                                logger.info("drop table column: {} {}", sql, batchExecutor.executeUpdate(sql));
                                dbMapping.removeTargetColumn(name);
                            } catch (Exception e) {
                                logger.warn("drop table column error: " + sql, e);
                            }
                        }
                    } else if (item instanceof SQLAlterTableAddColumn) {
                        SQLAlterTableAddColumn addColumn = (SQLAlterTableAddColumn) item;
                        if (!dbMapping.getMapAll()) {
                            logger.info("add table column disabled: {} {}", targetTable, addColumn.getColumns());
                            continue;
                        }
                        for (SQLColumnDefinition definition : addColumn.getColumns()) {
                            String name = Util.cleanColumn(definition.getNameAsString());
                            if (dbMapping.getExcludeColumns().contains(name)) {
                                continue;
                            }
                            String sql = "ALTER TABLE " + targetTable +
                                    " ADD IF NOT EXISTS " +
                                    dbMapping.escape(name) + " " + TypeUtil.getPhoenixType(definition, dbMapping.isLimit());
                            try {
                                logger.info("add table column: {} {}", sql, batchExecutor.executeUpdate(sql));
                                dbMapping.addTargetColumn(name, name);
                                if (definition.getDefaultExpr() != null) {
                                    String defVal = definition.getDefaultExpr().toString();
                                    if (!defVal.equalsIgnoreCase("NULL") && !defVal.equalsIgnoreCase("NOT NULL") && name.length() > 0) {
                                        defValues.put(name, defVal);
                                    }
                                }
                            } catch (Exception e) {
                                logger.error("add table column error: " + sql, e);
                                throw e;
                            }
                        }
                    }
                }
            }
        }
        if (!defValues.isEmpty()) {
            StringBuilder defSql = new StringBuilder();
            defSql.append("UPSERT INTO ").append(targetTable).append("(");
            Set<Map.Entry<String, String>> pkSet = dbMapping.getTargetPk().entrySet();
            Set<Map.Entry<String, String>> defSet = defValues.entrySet();
            for (Map.Entry<String, String> entry : pkSet) {
                defSql.append(dbMapping.escape(entry.getKey())).append(",");
            }
            for (Map.Entry<String, String> entry : defSet) {
                defSql.append(dbMapping.escape(entry.getKey())).append(",");
            }
            defSql.deleteCharAt(defSql.length() - 1).append(") SELECT ");
            for (Map.Entry<String, String> entry : pkSet) {
                defSql.append(dbMapping.escape(entry.getKey())).append(",");
            }
            for (Map.Entry<String, String> entry : defSet) {
                defSql.append(entry.getValue()).append(",");
            }
            defSql.deleteCharAt(defSql.length() - 1).append(" FROM ").append(targetTable);
            try {
                logger.info("set column default value: {} {}", defSql, batchExecutor.executeUpdate(defSql.toString()));
                batchExecutor.commit();
            } catch (SQLException e) {
                logger.error("set column default value error: {}", defSql, e);
                batchExecutor.rollback();
                throw e;
            }
        }
    }

    /**
     * 插入操作
     *
     * @param config 配置项
     * @param dml    DML数据
     */
    private void insert(BatchExecutor batchExecutor, MappingConfig config, SingleDml dml) throws SQLException {
        Map<String, Object> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }

        DbMapping dbMapping = config.getDbMapping();
        Map<String, String> columnsMap = SyncUtil.getColumnsMap(dbMapping, data);

        StringBuilder insertSql = new StringBuilder();
        insertSql.append("UPSERT INTO ").append(SyncUtil.getDbTableName(dbMapping)).append(" (");

        Map<String, Integer> ctype = getTargetColumnType(batchExecutor.getConn(), config);

        int mapLen = columnsMap.size();
        List<Map<String, ?>> values = new ArrayList<>();
        for (Map.Entry<String, String> entry : columnsMap.entrySet()) {
            String targetColumnName = entry.getKey();
            String srcColumnName = entry.getValue();
            if (srcColumnName == null) {
                srcColumnName = Util.cleanColumn(targetColumnName);
            }

            Integer type = ctype.get(Util.cleanColumn(targetColumnName).toLowerCase());
            if (type == null) {
                if (dbMapping.isSkipMissing()) {
                    logger.warn("Target missing field: {}", targetColumnName);
                    mapLen -= 1;
                    continue;
                } else if (dbMapping.getMapAll() && dbMapping.isAlter() && PhoenixEtlService.syncSchema(batchExecutor.getConn(), config)) {
                    columnsTypeCache.remove(config.getDestination() + "." + dbMapping.getDatabase() + "." + dbMapping.getTable());
                    ctype = getTargetColumnType(batchExecutor.getConn(), config);
                    type = ctype.get(Util.cleanColumn(targetColumnName).toLowerCase());
                }
                if (type == null) {
                    throw new RuntimeException("Target column: " + targetColumnName + " not matched");
                }
            }
            insertSql.append(dbMapping.escape(targetColumnName)).append(",");
            Object value = data.get(srcColumnName);
            BatchExecutor.setValue(values, type, value);
        }

        int len = insertSql.length();
        insertSql.delete(len - 1, len).append(") VALUES (");
        for (int i = 0; i < mapLen; i++) {
            insertSql.append("?,");
        }
        len = insertSql.length();
        insertSql.delete(len - 1, len).append(")");

        Map<String, Object> old = dml.getOld();
        try {
            if (old != null && !old.isEmpty()) {
                boolean keyChanged = false;
                List<Map<String, ?>> delValues = new ArrayList<>();
                StringBuilder deleteSql = new StringBuilder();
                deleteSql.append("DELETE FROM ").append(SyncUtil.getDbTableName(dbMapping)).append(" WHERE ");
                for (Map.Entry<String, String> entry : dbMapping.getTargetPk().entrySet()) {
                    String targetColumnName = entry.getKey();
                    String srcColumnName = entry.getValue();
                    if (srcColumnName == null) {
                        srcColumnName = Util.cleanColumn(targetColumnName);
                    }
                    Integer type = ctype.get(Util.cleanColumn(targetColumnName).toLowerCase());
                    if (type != null) {
                        deleteSql.append(dbMapping.escape(targetColumnName)).append("=? AND ");
                        // 如果有修改主键的情况
                        if (old.containsKey(srcColumnName)) {
                            keyChanged = true;
                            BatchExecutor.setValue(delValues, type, old.get(srcColumnName));
                        } else {
                            BatchExecutor.setValue(delValues, type, data.get(srcColumnName));
                        }
                    }
                }
                if (keyChanged) {
                    if (config.isDebug()) {
                        logger.info("insert into table: {} {}", deleteSql, delValues);
                    }
                    batchExecutor.execute(deleteSql.toString(), delValues);
                }
            }
            if (config.isDebug()) {
                logger.info("insert into table: {} {}", insertSql, values);
            }
            batchExecutor.execute(insertSql.toString(), values);
        } catch (SQLException | RuntimeException e) {
            logger.warn("Insert into target table, sql: {} {}", insertSql, values ,e);
            throw e;
        }
        if (logger.isTraceEnabled()) {
            logger.trace("Insert into target table, sql: {}", insertSql);
        }
    }

    /**
     * 删除操作 没有改动
     *
     * @param config MappingConfig
     * @param dml    Single DML
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
        try {
            batchExecutor.execute(sql.toString(), values);
            if (logger.isTraceEnabled()) {
                logger.trace("Delete from target table, sql: {}", sql);
            }
        } catch (SQLException e) {
            logger.warn("Delete from target error, sql: {} {}", sql, values);
            throw e;
        }
    }

    /**
     * truncate操作   没有改动
     *
     * @param config MappingConfig
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
     * @param conn   sql connection
     * @param config 映射配置
     * @return 字段sqlType
     */
    private Map<String, Integer> getTargetColumnType(Connection conn, MappingConfig config) {
        DbMapping dbMapping = config.getDbMapping();
        String cacheKey = config.getDestination() + "." + dbMapping.getDatabase() + "." + dbMapping.getTable();
        Map<String, Integer> columnType = columnsTypeCache.get(cacheKey);
        if (columnType == null) {
            synchronized (PhoenixSyncService.class) {
                columnType = columnsTypeCache.get(cacheKey);
                if (columnType == null) {
                    columnType = new LinkedHashMap<>();
                    final Map<String, Integer> columnTypeTmp = columnType;
                    String sql = "SELECT * FROM " + SyncUtil.getDbTableName(dbMapping) + " WHERE 1=2";
                    try {
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
                    } catch (RuntimeException e) {
                        //新增catch 里面做了操作
                        if (!e.getCause().getClass().getName().endsWith("TableNotFoundException")) {
                            throw e;
                        }
                        if (!PhoenixEtlService.syncSchema(conn, config)) {
                            throw e;
                        }
                        Util.sqlRS(conn, sql, rs -> {
                            try {
                                ResultSetMetaData rsd = rs.getMetaData();
                                int columnCount = rsd.getColumnCount();
                                for (int i = 1; i <= columnCount; i++) {
                                    columnTypeTmp.put(rsd.getColumnName(i).toLowerCase(), rsd.getColumnType(i));
                                }
                                columnsTypeCache.put(cacheKey, columnTypeTmp);
                            } catch (SQLException e1) {
                                logger.error(e1.getMessage(), e1);
                            }
                        });
                    }
                }
            }
        }
        return columnType;
    }

    /**
     * 拼接主键 where条件
     */
    private void appendCondition(DbMapping dbMapping, StringBuilder sql, Map<String, Integer> ctype,
                                 List<Map<String, ?>> values, Map<String, Object> d) {
        // 拼接主键
        for (Map.Entry<String, String> entry : dbMapping.getTargetPk().entrySet()) {
            String targetColumnName = entry.getKey();
            String srcColumnName = entry.getValue();
            if (srcColumnName == null) {
                srcColumnName = Util.cleanColumn(targetColumnName);
            }
            sql.append(dbMapping.escape(targetColumnName)).append("=? AND ");
            Integer type = ctype.get(Util.cleanColumn(targetColumnName).toLowerCase());
            if (type == null) {
                throw new RuntimeException("Target column: " + targetColumnName + " not matched");
            }
            BatchExecutor.setValue(values, type, d.get(srcColumnName));
        }
        int len = sql.length();
        sql.delete(len - 4, len);
    }

    public static class SyncItem {

        private MappingConfig config;
        private SingleDml singleDml;

        SyncItem(MappingConfig config, SingleDml singleDml) {
            this.config = config;
            this.singleDml = singleDml;
        }
    }

    /**
     * 取主键hash
     */
    private int pkHash(DbMapping dbMapping, Map<String, Object> d) {
        int hash = 0;
        // 取主键
        for (Map.Entry<String, String> entry : dbMapping.getTargetPk().entrySet()) {
            String targetColumnName = entry.getKey();
            String srcColumnName = entry.getValue();
            if (srcColumnName == null) {
                srcColumnName = Util.cleanColumn(targetColumnName);
            }
            Object value = null;
            if (d != null) {
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
