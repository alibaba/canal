package com.alibaba.otter.canal.client.adapter.rdb.service;

import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfig.DbMapping;
import com.alibaba.otter.canal.client.adapter.rdb.support.SimpleDml;
import com.alibaba.otter.canal.client.adapter.rdb.support.SyncUtil;
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

    private BatchExecutor[]                         batchExecutors;

    private int                                     threads            = 1;

    private ExecutorService[]                       threadExecutors;

    public RdbSyncService(Integer threads, DataSource dataSource){
        try {
            if (threads != null && threads > 1 && threads <= 10) {
                this.threads = threads;
            }
            batchExecutors = new BatchExecutor[this.threads];
            for (int i = 0; i < this.threads; i++) {
                Connection conn = dataSource.getConnection();
                conn.setAutoCommit(false);
                this.batchExecutors[i] = new BatchExecutor(i, conn);
            }
            threadExecutors = new ExecutorService[this.threads];
            for (int i = 0; i < this.threads; i++) {
                threadExecutors[i] = Executors.newFixedThreadPool(1);
            }
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    private List<SimpleDml>[] simpleDmls2Partition(List<SimpleDml> simpleDmlList) {
        List<SimpleDml>[] simpleDmlPartition = new ArrayList[threads];
        for (int i = 0; i < threads; i++) {
            simpleDmlPartition[i] = new ArrayList<>();
        }
        simpleDmlList.forEach(simpleDml -> {
            int hash;
            if (simpleDml.getConfig().getConcurrent()) {
                hash = pkHash(simpleDml.getConfig().getDbMapping(), simpleDml.getData(), threads);
            } else {
                hash = Math.abs(Math.abs(simpleDml.getConfig().getDbMapping().getTargetTable().hashCode()) % threads);
            }
            simpleDmlPartition[hash].add(simpleDml);
        });
        return simpleDmlPartition;
    }

    public void sync(List<SimpleDml> simpleDmlList) {
        try {
            List<SimpleDml>[] simpleDmlsPartition = simpleDmls2Partition(simpleDmlList);

            List<Future<Boolean>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                if (!simpleDmlsPartition[i].isEmpty()) {
                    int j = i;
                    futures.add(threadExecutors[i].submit(() -> {
                        simpleDmlsPartition[j].forEach(simpleDml -> sync(simpleDml, batchExecutors[j]));
                        batchExecutors[j].commit();
                        return true;
                    }));
                }
            }

            futures.forEach(future -> {
                try {
                    future.get();
                } catch (Exception e) {
                    // ignore
                }
            });
        } catch (Exception e) {
            logger.error("Error rdb sync for batch", e);
        }
    }

    public void sync(SimpleDml dml, BatchExecutor batchExecutor) {
        try {
            String type = dml.getType();
            if (type != null && type.equalsIgnoreCase("INSERT")) {
                insert(dml, batchExecutor);
            } else if (type != null && type.equalsIgnoreCase("UPDATE")) {
                update(dml, batchExecutor);
            } else if (type != null && type.equalsIgnoreCase("DELETE")) {
                delete(dml, batchExecutor);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 插入操作
     *
     * @param simpleDml DML数据
     */
    private void insert(SimpleDml simpleDml, BatchExecutor batchExecutor) {
        Map<String, Object> data = simpleDml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }

        DbMapping dbMapping = simpleDml.getConfig().getDbMapping();

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

        Map<String, Integer> ctype = getTargetColumnType(batchExecutor.getConn(), simpleDml.getConfig());

        String sql = insertSql.toString();

        try {
            List<Map<String, ?>> values = new ArrayList<>();

            for (Map.Entry<String, String> entry : columnsMap.entrySet()) {
                String targetColumnName = entry.getKey();
                String srcColumnName = entry.getValue();
                if (srcColumnName == null) {
                    srcColumnName = targetColumnName;
                }

                Integer type = ctype.get(targetColumnName.toLowerCase());
                if (type == null) {
                    throw new RuntimeException("No column: " + targetColumnName + " found in target db");
                }
                Object value = data.get(srcColumnName);
                BatchExecutor.setValue(values, type, value);
            }
            batchExecutor.execute(sql, values);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 更新操作
     * 
     * @param simpleDml DML数据
     */
    private void update(SimpleDml simpleDml, BatchExecutor batchExecutor) {
        Map<String, Object> data = simpleDml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }

        Map<String, Object> old = simpleDml.getOld();
        if (old == null || old.isEmpty()) {
            return;
        }

        DbMapping dbMapping = simpleDml.getConfig().getDbMapping();

        Map<String, String> columnsMap = SyncUtil.getColumnsMap(dbMapping, data);

        Map<String, Integer> ctype = getTargetColumnType(batchExecutor.getConn(), simpleDml.getConfig());

        try {
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
                        Integer type = ctype.get(targetColumnName.toLowerCase());
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
                logger.trace("Execute sql: {}", updateSql);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 删除操作
     * 
     * @param simpleDml
     */
    private void delete(SimpleDml simpleDml, BatchExecutor batchExecutor) {
        Map<String, Object> data = simpleDml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }

        DbMapping dbMapping = simpleDml.getConfig().getDbMapping();

        Map<String, Integer> ctype = getTargetColumnType(batchExecutor.getConn(), simpleDml.getConfig());

        try {
            StringBuilder sql = new StringBuilder();
            sql.append("DELETE FROM ").append(dbMapping.getTargetTable()).append(" WHERE ");

            List<Map<String, ?>> values = new ArrayList<>();

            // 拼接主键
            appendCondition(dbMapping, sql, ctype, values, data);

            batchExecutor.execute(sql.toString(), values);
            if (logger.isTraceEnabled()) {
                logger.trace("Execute sql: {}", sql);
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
     * 设置 preparedStatement
     * 
     * @param type sqlType
     * @param pstmt 需要设置的preparedStatement
     * @param value 值
     * @param i 索引号
     */
    public static void setPStmt(int type, PreparedStatement pstmt, Object value, int i) throws SQLException {
        if (value == null) {
            pstmt.setNull(i, type);
            return;
        }
        switch (type) {
            case Types.BIT:
            case Types.BOOLEAN:
                if (value instanceof Boolean) {
                    pstmt.setBoolean(i, (Boolean) value);
                } else if (value instanceof String) {
                    boolean v = !value.equals("0");
                    pstmt.setBoolean(i, v);
                } else if (value instanceof Number) {
                    boolean v = ((Number) value).intValue() != 0;
                    pstmt.setBoolean(i, v);
                } else {
                    pstmt.setNull(i, type);
                }
                break;
            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                pstmt.setString(i, value.toString());
                break;
            case Types.TINYINT:
                if (value instanceof Number) {
                    pstmt.setByte(i, ((Number) value).byteValue());
                } else if (value instanceof String) {
                    pstmt.setByte(i, Byte.parseByte((String) value));
                } else {
                    pstmt.setNull(i, type);
                }
                break;
            case Types.SMALLINT:
                if (value instanceof Number) {
                    pstmt.setShort(i, ((Number) value).shortValue());
                } else if (value instanceof String) {
                    pstmt.setShort(i, Short.parseShort((String) value));
                } else {
                    pstmt.setNull(i, type);
                }
                break;
            case Types.INTEGER:
                if (value instanceof Number) {
                    pstmt.setInt(i, ((Number) value).intValue());
                } else if (value instanceof String) {
                    pstmt.setInt(i, Integer.parseInt((String) value));
                } else {
                    pstmt.setNull(i, type);
                }
                break;
            case Types.BIGINT:
                if (value instanceof Number) {
                    pstmt.setLong(i, ((Number) value).longValue());
                } else if (value instanceof String) {
                    pstmt.setLong(i, Long.parseLong((String) value));
                } else {
                    pstmt.setNull(i, type);
                }
                break;
            case Types.DECIMAL:
            case Types.NUMERIC:
                pstmt.setBigDecimal(i, new BigDecimal(value.toString()));
                break;
            case Types.REAL:
                if (value instanceof Number) {
                    pstmt.setFloat(i, ((Number) value).floatValue());
                } else if (value instanceof String) {
                    pstmt.setFloat(i, Float.parseFloat((String) value));
                } else {
                    pstmt.setNull(i, type);
                }
                break;
            case Types.FLOAT:
            case Types.DOUBLE:
                if (value instanceof Number) {
                    pstmt.setDouble(i, ((Number) value).doubleValue());
                } else if (value instanceof String) {
                    pstmt.setDouble(i, Double.parseDouble((String) value));
                } else {
                    pstmt.setNull(i, type);
                }
                break;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:

                if (value instanceof byte[]) {
                    pstmt.setBytes(i, (byte[]) value);
                } else if (value instanceof String) {
                    pstmt.setBytes(i, ((String) value).getBytes(StandardCharsets.ISO_8859_1));
                } else {
                    pstmt.setNull(i, type);
                }
                break;
            case Types.CLOB:
                if (value instanceof byte[]) {
                    pstmt.setBytes(i, (byte[]) value);
                } else if (value instanceof String) {
                    Reader clobReader = new StringReader((String) value);
                    pstmt.setCharacterStream(i, clobReader);
                } else {
                    pstmt.setNull(i, type);
                }
                break;
            case Types.DATE:
                if (value instanceof java.util.Date) {
                    pstmt.setDate(i, new Date(((java.util.Date) value).getTime()));
                } else if (value instanceof String) {
                    String v = (String) value;
                    if (!v.startsWith("0000-00-00")) {
                        v = v.trim().replace(" ", "T");
                        DateTime dt = new DateTime(v);
                        pstmt.setDate(i, new Date(dt.toDate().getTime()));
                    } else {
                        pstmt.setNull(i, type);
                    }
                } else {
                    pstmt.setNull(i, type);
                }
                break;
            case Types.TIME:
                if (value instanceof java.util.Date) {
                    pstmt.setTime(i, new Time(((java.util.Date) value).getTime()));
                } else if (value instanceof String) {
                    String v = (String) value;
                    v = "T" + v;
                    DateTime dt = new DateTime(v);
                    pstmt.setTime(i, new Time(dt.toDate().getTime()));
                } else {
                    pstmt.setNull(i, type);
                }
                break;
            case Types.TIMESTAMP:
                if (value instanceof java.util.Date) {
                    pstmt.setTimestamp(i, new Timestamp(((java.util.Date) value).getTime()));
                } else if (value instanceof String) {
                    String v = (String) value;
                    if (!v.startsWith("0000-00-00")) {
                        v = v.trim().replace(" ", "T");
                        DateTime dt = new DateTime(v);
                        pstmt.setTimestamp(i, new Timestamp(dt.toDate().getTime()));
                    } else {
                        pstmt.setNull(i, type);
                    }
                } else {
                    pstmt.setNull(i, type);
                }
                break;
            default:
                pstmt.setObject(i, value, type);
        }
    }

    /**
     * 拼接主键 where条件
     */
    private static void appendCondition(DbMapping dbMapping, StringBuilder sql, Map<String, Integer> ctype,
                                        List<Map<String, ?>> values, Map<String, Object> d) {
        appendCondition(dbMapping, sql, ctype, values, d, null);
    }

    private static void appendCondition(DbMapping dbMapping, StringBuilder sql, Map<String, Integer> ctype,
                                        List<Map<String, ?>> values, Map<String, Object> d, Map<String, Object> o) {
        // 拼接主键
        for (Map.Entry<String, String> entry : dbMapping.getTargetPk().entrySet()) {
            String targetColumnName = entry.getKey();
            String srcColumnName = entry.getValue();
            if (srcColumnName == null) {
                srcColumnName = targetColumnName;
            }
            sql.append(targetColumnName).append("=? AND ");
            Integer type = ctype.get(targetColumnName.toLowerCase());
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
     * 取主键hash
     */
    private static int pkHash(DbMapping dbMapping, Map<String, Object> d, int threads) {
        return pkHash(dbMapping, d, null, threads);
    }

    private static int pkHash(DbMapping dbMapping, Map<String, Object> d, Map<String, Object> o, int threads) {
        int hash = 0;
        // 取主键
        for (Map.Entry<String, String> entry : dbMapping.getTargetPk().entrySet()) {
            String targetColumnName = entry.getKey();
            String srcColumnName = entry.getValue();
            if (srcColumnName == null) {
                srcColumnName = targetColumnName;
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
        for (BatchExecutor batchExecutor : batchExecutors) {
            batchExecutor.close();
        }
        for (ExecutorService executorService : threadExecutors) {
            executorService.shutdown();
        }
    }
}
