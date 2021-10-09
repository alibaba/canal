package com.alibaba.otter.canal.client.adapter.phoenix.service;

import com.alibaba.otter.canal.client.adapter.phoenix.PhoenixAdapter;
import com.alibaba.otter.canal.client.adapter.phoenix.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.phoenix.config.MappingConfig.DbMapping;
import com.alibaba.otter.canal.client.adapter.phoenix.support.PhoenixSupportUtil;
import com.alibaba.otter.canal.client.adapter.phoenix.support.SyncUtil;
import com.alibaba.otter.canal.client.adapter.phoenix.support.TypeUtil;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.Util;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Phoenix ETL 操作业务类
 */
public class PhoenixEtlService {

    private static final Logger logger = LoggerFactory.getLogger(PhoenixEtlService.class);

    private static String[] splitNotEmpty(String s) {
        if (s != null && s.trim().length() > 0) {
            return s.trim().split(",");
        }
        return new String[]{};
    }


    static boolean syncSchema(Connection targetDSConnection, MappingConfig config) {
        DataSource srcDataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        if (srcDataSource == null) {
            return false;
        }
        try {
            return syncSchema(srcDataSource.getConnection(), targetDSConnection, config);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean syncSchema(DataSource srcDS,Connection targetDSConnection, MappingConfig config) {
        try {
            return syncSchema(srcDS.getConnection(),targetDSConnection, config);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static boolean syncSchema(Connection srcDS, Connection targetDS, MappingConfig config) {
        DbMapping dbMapping = config.getDbMapping();
        if (dbMapping.getMapAll() && dbMapping.isAlter()) { // 检查字段是否缺失
            Map<String, Integer> targetColumnType = new LinkedHashMap<>();
            String targetTable = SyncUtil.getDbTableName(dbMapping);
            try {
                Util.sqlRS(targetDS, "SELECT * FROM " + targetTable + " LIMIT 1", rs -> {
                    try {
                        ResultSetMetaData rsd = rs.getMetaData();
                        int columnCount = rsd.getColumnCount();
                        for (int i = 1; i <= columnCount; i++) {
                            targetColumnType.put(rsd.getColumnName(i).toLowerCase(), rsd.getColumnType(i));
                        }
                    } catch (Exception e) {
                        logger.error(dbMapping.getTable() + " etl failed! ==>" + e.getMessage(), e);
                    }
                });
            } catch (RuntimeException e) {
                if (!e.getCause().getClass().getName().endsWith("TableNotFoundException")) {
                    throw e;
                }
            }
            StringBuilder missing = new StringBuilder();
            StringBuilder constraint = new StringBuilder();
            Util.sqlRS(srcDS, "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + dbMapping.getDatabase() + "'  AND TABLE_NAME = '" + dbMapping.getTable() + "'", rs -> {
                try {
                    List<String> excludeColumns = config.getDbMapping().getExcludeColumns();
                    while (rs.next()) {
                        String name = rs.getString("COLUMN_NAME");
                        String lower = name.toLowerCase();
                        String colType = rs.getString("COLUMN_TYPE");
                        if (targetColumnType.get(lower) == null && !excludeColumns.contains(lower)) {
                            boolean isPri = rs.getString("COLUMN_KEY").equals("PRI");
                            String[] args = splitNotEmpty(colType.replaceAll("^\\w+(?:\\(([^)]*)\\))?[\\s\\S]*$", "$1"));
                            missing.append(dbMapping.escape(name)).append(" ").append(TypeUtil.getPhoenixType(
                                    rs.getString("DATA_TYPE").toUpperCase(),
                                    args,
                                    colType.contains("unsigned"),
                                    dbMapping.isLimit()
                            ));
                            if (isPri) {
                                if (args.length > 0 && dbMapping.isLimit() || rs.getString("IS_NULLABLE").equals("NO")) {
                                    missing.append(" NOT NULL");
                                }
                                constraint.append(dbMapping.escape(name)).append(',');
                            }
                            missing.append(',');
                        }
                    }
                } catch (Exception e) {
                    logger.error(dbMapping.getDatabase() + "." + dbMapping.getTable() + " schema failed! ==>" + e.getMessage(), e);
                    throw new RuntimeException(e);
                }
            });
            if (missing.length() > 0) {
                String sql;
                if (targetColumnType.isEmpty()) {
                    if (constraint.length() > 0) {
                        constraint.deleteCharAt(constraint.length() - 1);
                        missing.append("CONSTRAINT pk PRIMARY KEY(").append(constraint.toString()).append(")");
                    } else {
                        missing.deleteCharAt(missing.length() - 1);
                    }
                    sql = "CREATE TABLE " + targetTable + " (" + missing.toString() + ")";
                } else {
                    missing.deleteCharAt(missing.length() - 1);
                    sql = "ALTER TABLE " + targetTable + " ADD " + missing.toString();
                }
                logger.info("schema missing: {} {}", targetColumnType, sql);
                try (PreparedStatement pstmt = targetDS.prepareStatement(sql)) {
                    pstmt.executeUpdate();
                } catch (SQLException e) {
                    logger.error("sync schema error: " + e.getMessage(), e);
                }
            } else {
                logger.debug("schema ok: {}", targetColumnType);
            }
            return true;
        }
        return false;
    }

    /**
     * 导入数据
     */
    public static EtlResult importData(DataSource srcDS, Connection targetDSConnection, MappingConfig config,
                                       List<String> params) {
        EtlResult etlResult = new EtlResult();
        AtomicLong successCount = new AtomicLong();
        List<String> errMsg = new ArrayList<>();
        String hbaseTable = "";
        try {
            if (config == null) {
                logger.error("Config is null!");
                etlResult.setSucceeded(false);
                etlResult.setErrorMessage("Config is null!");
                return etlResult;
            }
            boolean debug = params != null && params.get(0).equals("_debug");
            if (debug) {
                params = params.subList(1, params.size());
            }
            syncSchema(srcDS, targetDSConnection, config);
            DbMapping dbMapping = config.getDbMapping();

            long start = System.currentTimeMillis();

            // 拼接sql
            StringBuilder sql = new StringBuilder(
                    "SELECT * FROM " + dbMapping.getDatabase() + "." + dbMapping.getTable());

            // 拼接条件
            appendCondition(params, dbMapping, srcDS, sql);

            // 获取总数
            String countSql = "SELECT COUNT(1) FROM ( " + sql + ") _CNT ";
            long cnt = (Long) Util.sqlRS(srcDS, countSql, rs -> {
                Long count = null;
                try {
                    if (rs.next()) {
                        count = ((Number) rs.getObject(1)).longValue();
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
                return count == null ? 0 : count;
            });

            // 当大于1万条记录时开启多线程
            if (cnt >= 10000) {
                int threadCount = 3;
                long perThreadCnt = cnt / threadCount;
                ExecutorService executor = Util.newFixedThreadPool(threadCount, 5000L);
                for (int i = 0; i < threadCount; i++) {
                    long offset = i * perThreadCnt;
                    Long size = null;
                    if (i != threadCount - 1) {
                        size = perThreadCnt;
                    }
                    String sqlFinal;
                    if (size != null) {
                        sqlFinal = sql + " LIMIT " + offset + "," + size;
                    } else {
                        sqlFinal = sql + " LIMIT " + offset + "," + cnt;
                    }
                    executor
                            .execute(() -> executeSqlImport(srcDS, targetDSConnection, sqlFinal, dbMapping, successCount, errMsg, debug));
                }

                executor.shutdown();
                //noinspection StatementWithEmptyBody
                while (!executor.awaitTermination(3, TimeUnit.SECONDS)) ;
            } else {
                executeSqlImport(srcDS, targetDSConnection, sql.toString(), dbMapping, successCount, errMsg, debug);
            }

            logger.info(
                    dbMapping.getTable() + " etl completed in: " + (System.currentTimeMillis() - start) / 1000 + "s!");

            etlResult
                    .setResultMessage("导入目标表 " + SyncUtil.getDbTableName(dbMapping) + " 数据：" + successCount.get() + " 条");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            errMsg.add(hbaseTable + " etl failed! ==>" + e.getMessage());
        }

        if (errMsg.isEmpty()) {
            etlResult.setSucceeded(true);
        } else {
            etlResult.setErrorMessage(Joiner.on("\n").join(errMsg));
        }
        return etlResult;
    }

    private static void appendCondition(List<String> params, DbMapping dbMapping, DataSource ds,
                                        StringBuilder sql) {
        if (params != null && params.size() == 1 && dbMapping.getEtlCondition() == null) {
            AtomicBoolean stExists = new AtomicBoolean(false);
            // 验证是否有SYS_TIME字段
            Util.sqlRS(ds, sql.toString(), rs -> {
                try {
                    ResultSetMetaData rsmd = rs.getMetaData();
                    int cnt = rsmd.getColumnCount();
                    for (int i = 1; i <= cnt; i++) {
                        String columnName = rsmd.getColumnName(i);
                        if ("SYS_TIME".equalsIgnoreCase(columnName)) {
                            stExists.set(true);
                            break;
                        }
                    }
                } catch (Exception e) {
                    // ignore
                }
                return null;
            });
            if (stExists.get()) {
                sql.append(" WHERE SYS_TIME >= '").append(params.get(0)).append("' ");
            }
        } else if (dbMapping.getEtlCondition() != null && params != null && params.size() > 0) {
            String etlCondition = dbMapping.getEtlCondition();
            int size = params.size();
            for (int i = 0; i < size; i++) {
                etlCondition = etlCondition.replace("{" + i + "}", params.get(i));
            }

            sql.append(" ").append(etlCondition);
        }
    }

    /**
     * 执行导入
     */
    private static boolean executeSqlImport(DataSource srcDS, Connection targetDSConnection, String sql, DbMapping dbMapping,
                                            AtomicLong successCount, List<String> errMsg, boolean debug) {
        try {
            Map<String, String> columnsMap = new LinkedHashMap<>();
            Map<String, Integer> columnType = new LinkedHashMap<>();


            PhoenixSupportUtil.sqlRS(targetDSConnection, "SELECT * FROM " + SyncUtil.getDbTableName(dbMapping) + " LIMIT 1 ", rs -> {
                try {

                    ResultSetMetaData rsd = rs.getMetaData();
                    int columnCount = rsd.getColumnCount();
                    List<String> columns = new ArrayList<>();
                    List<String> excludeColumns = dbMapping.getExcludeColumns();
                    for (int i = 1; i <= columnCount; i++) {
                        String lower = rsd.getColumnName(i).toLowerCase();
                        if (!excludeColumns.contains(lower)) {
                            columnType.put(lower, rsd.getColumnType(i));
                            columns.add(lower);
                        }
                    }
                    columnsMap.putAll(SyncUtil.getColumnsMap(dbMapping, columns));
                    return true;
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    return false;
                }
            });
            Util.sqlRS(srcDS, sql, rs -> {
                int idx = 1;

                try {
                    boolean completed = false;

                    // if (dbMapping.isMapAll()) {
                    // columnsMap = dbMapping.getAllColumns();
                    // } else {
                    // columnsMap = dbMapping.getTargetColumns();
                    // }

                    StringBuilder insertSql = new StringBuilder();
                    insertSql.append("UPSERT INTO ").append(SyncUtil.getDbTableName(dbMapping)).append(" (");
                    columnsMap
                            .forEach((targetColumnName, srcColumnName) -> insertSql.append(dbMapping.escape(targetColumnName)).append(","));

                    int len = insertSql.length();
                    insertSql.delete(len - 1, len).append(") VALUES (");
                    int mapLen = columnsMap.size();
                    for (int i = 0; i < mapLen; i++) {
                        insertSql.append("?,");
                    }
                    len = insertSql.length();
                    insertSql.delete(len - 1, len).append(")");
                    try (
                         //Connection connTarget = targetDS.getConnection();
                         Connection connTarget =PhoenixAdapter.getPhoenixConnection();
                         PreparedStatement pstmt = connTarget.prepareStatement(insertSql.toString())) {
                        connTarget.setAutoCommit(false);

                        while (rs.next()) {
                            completed = false;

                            pstmt.clearParameters();

                            // 删除数据
                            Map<String, Object> values = new LinkedHashMap<>();
                            StringBuilder deleteSql = new StringBuilder(
                                    "DELETE FROM " + SyncUtil.getDbTableName(dbMapping) + " WHERE ");
                            appendCondition(dbMapping, deleteSql, values, rs);
                            try (PreparedStatement pstmt2 = connTarget.prepareStatement(deleteSql.toString())) {
                                int k = 1;
                                for (Object val : values.values()) {
                                    pstmt2.setObject(k++, val);
                                }
                                pstmt2.execute();
                            }

                            Map<String, Object> insertValues = new HashMap<>();
                            int i = 1;
                            for (Map.Entry<String, String> entry : columnsMap.entrySet()) {
                                String targetClolumnName = entry.getKey();
                                String srcColumnName = entry.getValue();
                                if (srcColumnName == null) {
                                    srcColumnName = targetClolumnName;
                                }

                                Integer type = columnType.get(targetClolumnName.toLowerCase());

                                try {
                                    Object value = rs.getObject(srcColumnName);
                                    insertValues.put(srcColumnName, value);
                                    if (value != null) {
                                        SyncUtil.setPStmt(type, pstmt, value, i);
                                    } else {
                                        pstmt.setNull(i, type);
                                    }
                                } catch (SQLException e) {
                                    insertValues.put(srcColumnName, null);
                                    pstmt.setNull(i, type);
                                }
                                i++;
                            }
                            if (debug) {
                                logger.info("insert sql: {} {} {}", insertSql, insertValues, pstmt);
                            }

                            pstmt.execute();
                            if (logger.isTraceEnabled()) {
                                logger.trace("Insert into target table, sql: {}", insertSql);
                            }

                            if (idx % dbMapping.getCommitBatch() == 0) {
                                connTarget.commit();
                                completed = true;
                            }
                            idx++;
                            successCount.incrementAndGet();
                            if (logger.isDebugEnabled()) {
                                logger.debug("successful import count:" + successCount.get());
                            }
                        }
                        if (!completed) {
                            connTarget.commit();
                        }
                    }

                } catch (Exception e) {
                    logger.error(dbMapping.getTable() + " etl failed! ==>" + e.getMessage(), e);
                    errMsg.add(dbMapping.getTable() + " etl failed! ==>" + e.getMessage());
                }
                return idx;
            });
            return true;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        }
    }

    /**
     * 拼接目标表主键where条件
     */
    private static void appendCondition(DbMapping dbMapping, StringBuilder sql, Map<String, Object> values,
                                        ResultSet rs) throws SQLException {
        // 拼接主键
        for (Map.Entry<String, String> entry : dbMapping.getTargetPk().entrySet()) {
            String targetColumnName = entry.getKey();
            String srcColumnName = entry.getValue();
            if (srcColumnName == null) {
                srcColumnName = targetColumnName;
            }
            sql.append(dbMapping.escape(targetColumnName)).append("=? AND ");
            values.put(targetColumnName, rs.getObject(srcColumnName));
        }
        int len = sql.length();
        sql.delete(len - 4, len);
    }
}
