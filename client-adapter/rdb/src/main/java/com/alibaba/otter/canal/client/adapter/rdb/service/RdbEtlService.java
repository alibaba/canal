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
import java.util.concurrent.atomic.AtomicLong;

import javax.sql.DataSource;

import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfig.DbMapping;
import com.alibaba.otter.canal.client.adapter.rdb.support.SyncUtil;
import com.alibaba.otter.canal.client.adapter.support.AbstractEtlService;
import com.alibaba.otter.canal.client.adapter.support.AdapterConfig;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.Util;

/**
 * RDB ETL 操作业务类
 *
 * @author rewerma @ 2018-11-7
 * @version 1.0.0
 */
public class RdbEtlService extends AbstractEtlService {

    private DataSource    targetDS;
    private MappingConfig config;

    public RdbEtlService(DataSource targetDS, MappingConfig config){
        super("RDB", config);
        this.targetDS = targetDS;
        this.config = config;
    }

    /**
     * 导入数据
     */
    public EtlResult importData(List<String> params) {
        DbMapping dbMapping = config.getDbMapping();
        String sql = "SELECT * FROM " + dbMapping.getDatabase() + "." + dbMapping.getTable();
        return importData(sql, params);
    }

    /**
     * 执行导入
     */
    protected boolean executeSqlImport(DataSource srcDS, String sql, List<Object> values,
                                       AdapterConfig.AdapterMapping mapping, AtomicLong impCount, List<String> errMsg) {
        try {
            DbMapping dbMapping = (DbMapping) mapping;
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

            Util.sqlRS(srcDS, sql, values, rs -> {
                int idx = 1;

                try {
                    boolean completed = false;

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
                            Map<String, Object> pkVal = new LinkedHashMap<>();
                            StringBuilder deleteSql = new StringBuilder(
                                "DELETE FROM " + SyncUtil.getDbTableName(dbMapping) + " WHERE ");
                            appendCondition(dbMapping, deleteSql, pkVal, rs);
                            try (PreparedStatement pstmt2 = connTarget.prepareStatement(deleteSql.toString())) {
                                int k = 1;
                                for (Object val : pkVal.values()) {
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
                            impCount.incrementAndGet();
                            if (logger.isDebugEnabled()) {
                                logger.debug("successful import count:" + impCount.get());
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
