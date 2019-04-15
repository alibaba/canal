package com.alibaba.otter.canal.client.adapter.rdb.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfig.DbMapping;
import com.alibaba.otter.canal.client.adapter.rdb.support.SyncUtil;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.Util;
import com.google.common.base.Joiner;

/**
 * RDB ETL 操作业务类
 *
 * @author rewerma @ 2018-11-7
 * @version 1.0.0
 */
public class RdbEtlService {

    private static final Logger logger = LoggerFactory.getLogger(RdbEtlService.class);

    /**
     * 导入数据
     */
    public static EtlResult importData(DataSource srcDS, DataSource targetDS, MappingConfig config,
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
                        .execute(() -> executeSqlImport(srcDS, targetDS, sqlFinal, dbMapping, successCount, errMsg));
                }

                executor.shutdown();
                while (!executor.awaitTermination(3, TimeUnit.SECONDS)) {
                    // ignore
                }
            } else {
                executeSqlImport(srcDS, targetDS, sql.toString(), dbMapping, successCount, errMsg);
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
                                        StringBuilder sql) throws SQLException {
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
        } else if (dbMapping.getEtlCondition() != null && params != null) {
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
    private static boolean executeSqlImport(DataSource srcDS, DataSource targetDS, String sql, DbMapping dbMapping,
                                            AtomicLong successCount, List<String> errMsg) {
        try {
            Map<String, String> columnsMap = new LinkedHashMap<>();
            Map<String, Integer> columnType = new LinkedHashMap<>();

            Util.sqlRS(targetDS, "SELECT * FROM " + SyncUtil.getDbTableName(dbMapping) + " LIMIT 1 ", rs -> {
                try {

                    ResultSetMetaData rsd = rs.getMetaData();
                    int columnCount = rsd.getColumnCount();
                    List<String> columns = new ArrayList<>();
                    for (int i = 1; i <= columnCount; i++) {
                        columnType.put(rsd.getColumnName(i).toLowerCase(), rsd.getColumnType(i));
                        columns.add(rsd.getColumnName(i));
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
                    insertSql.append("INSERT INTO ").append(SyncUtil.getDbTableName(dbMapping)).append(" (");
                    columnsMap
                        .forEach((targetColumnName, srcColumnName) -> insertSql.append(targetColumnName).append(","));

                    int len = insertSql.length();
                    insertSql.delete(len - 1, len).append(") VALUES (");
                    int mapLen = columnsMap.size();
                    for (int i = 0; i < mapLen; i++) {
                        insertSql.append("?,");
                    }
                    len = insertSql.length();
                    insertSql.delete(len - 1, len).append(")");
                    try (Connection connTarget = targetDS.getConnection();
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

                            int i = 1;
                            for (Map.Entry<String, String> entry : columnsMap.entrySet()) {
                                String targetClolumnName = entry.getKey();
                                String srcColumnName = entry.getValue();
                                if (srcColumnName == null) {
                                    srcColumnName = targetClolumnName;
                                }

                                Integer type = columnType.get(targetClolumnName.toLowerCase());

                                Object value = rs.getObject(srcColumnName);
                                if (value != null) {
                                    SyncUtil.setPStmt(type, pstmt, value, i);
                                } else {
                                    pstmt.setNull(i, type);
                                }

                                i++;
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
            sql.append(targetColumnName).append("=? AND ");
            values.put(targetColumnName, rs.getObject(srcColumnName));
        }
        int len = sql.length();
        sql.delete(len - 4, len);
    }
}
