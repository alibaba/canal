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
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import javax.sql.DataSource;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfig.DbMapping;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;

/**
 * RDB同步操作业务
 *
 * @author rewerma 2018-11-7 下午06:45:49
 * @version 1.0.0
 */
public class RdbSyncService {

    private static final Logger                            logger             = LoggerFactory
        .getLogger(RdbSyncService.class);

    private static final Map<String, Map<String, Integer>> COLUMNS_TYPE_CACHE = new ConcurrentHashMap<>();

    private DataSource                                     dataSource;

    public RdbSyncService(DataSource dataSource){
        this.dataSource = dataSource;
    }

    public void sync(MappingConfig config, Dml dml) {
        try {
            if (config != null) {
                {
                    DbMapping dbMapping = config.getDbMapping();
                    // 从源表加载所有字段名
                    if (dbMapping.getAllColumns() == null) {
                        synchronized (RdbSyncService.class) {
                            if (dbMapping.getAllColumns() == null) {
                                DataSource srcDS = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
                                Connection srcConn = srcDS.getConnection();
                                String srcMetaSql = "SELECT * FROM " + dbMapping.getDatabase() + "."
                                                    + dbMapping.getTable() + " WHERE 1=2 ";
                                List<String> srcColumns = new ArrayList<>();
                                sqlRS(srcConn, srcMetaSql, rs -> {
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
                                        for (Map.Entry<String, String> entry : dbMapping.getTargetColumns()
                                            .entrySet()) {
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
                }

                String type = dml.getType();
                if (type != null && type.equalsIgnoreCase("INSERT")) {
                    insert(config, dml);
                } else if (type != null && type.equalsIgnoreCase("UPDATE")) {
                    // update(config, dml);
                } else if (type != null && type.equalsIgnoreCase("DELETE")) {
                    // delete(config, dml);
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("DML: {}", JSON.toJSONString(dml));
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void update(MappingConfig config, Dml dml) throws SQLException {
        List<Map<String, Object>> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }

        List<Map<String, Object>> old = dml.getOld();
        if (old == null || old.isEmpty()) {
            return;
        }

        DbMapping dbMapping = config.getDbMapping();

        int idx = 1;
        boolean complete = false;

        Connection conn = dataSource.getConnection();
        boolean oriAutoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);

        try {
            Map<String, String> columnsMap;
            if (dbMapping.isMapAll()) {
                columnsMap = dbMapping.getAllColumns();
            } else {
                columnsMap = dbMapping.getTargetColumns();
            }

            int i = 0;
            for (Map<String, Object> o : old) {
                Map<String, Object> d = data.get(i);
                StringBuilder updateSql = new StringBuilder();
                updateSql.append("UPDATE ").append(dbMapping.getTargetTable()).append(" SET ");
                List<Object> values = new ArrayList<>();
                boolean flag = false;
                for (String srcColumnName : o.keySet()) {
                    List<String> targetColumnNames = new ArrayList<>();
                    columnsMap.forEach((targetColumn, srcColumn) -> {
                        if (srcColumnName.toLowerCase().equals(srcColumn)) {
                            targetColumnNames.add(targetColumn);
                        }
                    });
                    if (!targetColumnNames.isEmpty()) {
                        if (!flag) {
                            flag = true;
                        }
                        for (String targetColumnName : targetColumnNames) {
                            updateSql.append(targetColumnName).append("=?, ");
                            values.add(data.get(i));
                        }
                    }
                }
                if (flag) {
                    int len = updateSql.length();
                    updateSql.delete(len - 2, len).append(" WHERE ");
                }
                i++;
            }
        } catch (Exception e) {
            conn.rollback();
        } finally {
            conn.setAutoCommit(oriAutoCommit);
            conn.close();
        }
    }

    /**
     * 插入操作
     *
     * @param config 配置项
     * @param dml DML数据
     */
    private void insert(MappingConfig config, Dml dml) throws SQLException {
        List<Map<String, Object>> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }

        DbMapping dbMapping = config.getDbMapping();

        int idx = 1;
        boolean completed = false;

        Connection conn = dataSource.getConnection();
        boolean oriAutoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);
        try {
            Map<String, String> columnsMap;
            if (dbMapping.isMapAll()) {
                columnsMap = dbMapping.getAllColumns();
            } else {
                columnsMap = dbMapping.getTargetColumns();
            }

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

            PreparedStatement pstmt = conn.prepareStatement(insertSql.toString());

            for (Map<String, Object> r : data) {
                pstmt.clearParameters();
                convertData2DbRow(conn, config, r, pstmt);

                pstmt.execute();
                if (logger.isTraceEnabled()) {
                    logger.trace("Insert into target db, sql: {}", insertSql);
                }

                if (idx % config.getDbMapping().getCommitBatch() == 0) {
                    conn.commit();
                    completed = true;
                }
                idx++;
            }
            if (!completed) {
                conn.commit();
            }
        } catch (Exception e) {
            conn.rollback();
        } finally {
            conn.setAutoCommit(oriAutoCommit);
            conn.close();
        }
    }

    private static void sqlRS(Connection conn, String sql, Consumer<ResultSet> consumer) {
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            consumer.accept(rs);
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }
    }

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
                    sqlRS(conn, sql, rs -> {
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
     * 新增类型转换
     *
     * @param config
     * @param data
     * @param pstmt
     * @throws SQLException
     */
    private void convertData2DbRow(Connection conn, MappingConfig config, Map<String, Object> data,
                                   PreparedStatement pstmt) throws SQLException {
        DbMapping dbMapping = config.getDbMapping();
        Map<String, String> columnsMap;
        if (dbMapping.isMapAll()) {
            columnsMap = dbMapping.getAllColumns();
        } else {
            columnsMap = dbMapping.getTargetColumns();
        }
        Map<String, Integer> ctype = getTargetColumnType(conn, config);

        int i = 1;
        for (Map.Entry<String, String> entry : columnsMap.entrySet()) {
            String targetClassName = entry.getKey();
            String srcColumnName = entry.getValue();
            if (srcColumnName == null) {
                srcColumnName = targetClassName;
            }

            Integer type = ctype.get(targetClassName.toLowerCase());

            Object value = data.get(srcColumnName);
            if (value != null) {
                if (type == null) {
                    throw new RuntimeException("No column: " + targetClassName + " found in target db");
                }

                setPStmt(type, pstmt, value, i);
            } else {
                pstmt.setNull(i, type);
            }
            i++;
        }
    }

    private void setPStmt(int type, PreparedStatement pstmt, Object value, int i) throws SQLException {
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
                    pstmt.setDate(i, new java.sql.Date(((java.util.Date) value).getTime()));
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
                    pstmt.setTime(i, new java.sql.Time(((java.util.Date) value).getTime()));
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
                    pstmt.setTimestamp(i, new java.sql.Timestamp(((java.util.Date) value).getTime()));
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
}
