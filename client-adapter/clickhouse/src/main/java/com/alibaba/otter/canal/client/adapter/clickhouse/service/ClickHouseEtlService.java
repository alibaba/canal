package com.alibaba.otter.canal.client.adapter.clickhouse.service;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.sql.DataSource;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.client.adapter.clickhouse.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.clickhouse.config.MappingConfig.DbMapping;
import com.alibaba.otter.canal.client.adapter.clickhouse.support.SyncUtil;
import com.alibaba.otter.canal.client.adapter.support.*;

/**
 * ClickHouse ETL 操作业务类
 *
 * @author: Xander
 * @date: Created in 2023/11/10 22:23
 * @email: zhrunxin33@gmail.com
 * @version 1.1.8
 */
public class ClickHouseEtlService extends AbstractEtlService {

    private DataSource    targetDS;
    private MappingConfig config;

    public ClickHouseEtlService(DataSource targetDS, MappingConfig config){
        super("CLICKHOUSE", config);
        this.targetDS = targetDS;
        this.config = config;
    }

    /**
     * 导入数据
     */
    public EtlResult importData(List<String> params) {
        DbMapping dbMapping = config.getDbMapping();
        DruidDataSource dataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        String sql = "SELECT * FROM " + SyncUtil.getSourceDbTableName(dbMapping, dataSource.getDbType());
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
            DruidDataSource dataSource = (DruidDataSource) srcDS;
            String backtick = SyncUtil.getBacktickByDbType(dataSource.getDbType());

            Util.sqlRS(targetDS,
                "SELECT * FROM " + SyncUtil.getDbTableName(dbMapping, dataSource.getDbType()) + " LIMIT 1 ",
                rs -> {
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
                    insertSql.append("INSERT INTO ")
                        .append(SyncUtil.getDbTableName(dbMapping, dataSource.getDbType()))
                        .append(" (");
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
                    logger.info("executeSqlImport sql:{}", insertSql.toString());
                    try (Connection connTarget = targetDS.getConnection();
                            PreparedStatement pstmt = connTarget.prepareStatement(insertSql.toString())) {
                        connTarget.setAutoCommit(false);

                        while (rs.next()) {
                            completed = false;

                            pstmt.clearParameters();

                            // 删除数据
                            Map<String, Object> pkVal = new LinkedHashMap<>();
                            StringBuilder deleteSql = new StringBuilder(
                                "ALTER TABLE " + SyncUtil.getDbTableName(dbMapping, dataSource.getDbType())
                                                                        + " DELETE WHERE ");
                            appendCondition(dbMapping, deleteSql, pkVal, rs, backtick);
                            try (PreparedStatement pstmt2 = connTarget.prepareStatement(deleteSql.toString())) {
                                int k = 1;
                                for (Object val : pkVal.values()) {
                                    pstmt2.setObject(k++, val);
                                }
                                pstmt2.execute();
                            }

                            int i = 1;
                            for (Map.Entry<String, String> entry : columnsMap.entrySet()) {
                                String targetColumnName = entry.getKey();
                                String srcColumnName = entry.getValue();
                                if (srcColumnName == null) {
                                    srcColumnName = targetColumnName;
                                }

                                Integer type = columnType.get(targetColumnName.toLowerCase());
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
                                        ResultSet rs, String backtick) throws SQLException {
        // 拼接主键
        for (Map.Entry<String, String> entry : dbMapping.getTargetPk().entrySet()) {
            String targetColumnName = entry.getKey();
            String srcColumnName = entry.getValue();
            if (srcColumnName == null) {
                srcColumnName = targetColumnName;
            }
            sql.append(backtick).append(targetColumnName).append(backtick).append("=? AND ");
            values.put(targetColumnName, rs.getObject(srcColumnName));
        }
        int len = sql.length();
        sql.delete(len - 4, len);
    }
}
