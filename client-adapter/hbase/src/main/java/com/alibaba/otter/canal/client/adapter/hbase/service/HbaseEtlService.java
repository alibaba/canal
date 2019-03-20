package com.alibaba.otter.canal.client.adapter.hbase.service;

import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.sql.DataSource;

import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.adapter.hbase.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.hbase.support.HRow;
import com.alibaba.otter.canal.client.adapter.hbase.support.HbaseTemplate;
import com.alibaba.otter.canal.client.adapter.hbase.support.PhType;
import com.alibaba.otter.canal.client.adapter.hbase.support.PhTypeUtil;
import com.alibaba.otter.canal.client.adapter.hbase.support.Type;
import com.alibaba.otter.canal.client.adapter.hbase.support.TypeUtil;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.JdbcTypeUtil;
import com.alibaba.otter.canal.client.adapter.support.Util;
import com.google.common.base.Joiner;

/**
 * HBase ETL 操作业务类
 *
 * @author rewerma @ 2018-10-20
 * @version 1.0.0
 */
public class HbaseEtlService {

    private static Logger logger = LoggerFactory.getLogger(HbaseEtlService.class);

    /**
     * 建表
     *
     * @param hbaseTemplate
     * @param config
     */
    public static void createTable(HbaseTemplate hbaseTemplate, MappingConfig config) {
        try {
            // 判断hbase表是否存在，不存在则建表
            MappingConfig.HbaseMapping hbaseMapping = config.getHbaseMapping();
            if (!hbaseTemplate.tableExists(hbaseMapping.getHbaseTable())) {
                hbaseTemplate.createTable(hbaseMapping.getHbaseTable(), hbaseMapping.getFamily());
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    /**
     * 导入数据
     *
     * @param ds 数据源
     * @param hbaseTemplate hbaseTemplate
     * @param config 配置
     * @param params 筛选条件
     * @return 导入结果
     */
    public static EtlResult importData(DataSource ds, HbaseTemplate hbaseTemplate, MappingConfig config,
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
            MappingConfig.HbaseMapping hbaseMapping = config.getHbaseMapping();
            hbaseTable = hbaseMapping.getHbaseTable();

            long start = System.currentTimeMillis();

            if (params != null && params.size() == 1 && "rebuild".equalsIgnoreCase(params.get(0))) {
                logger.info(hbaseMapping.getHbaseTable() + " rebuild is starting!");
                // 如果表存在则删除
                if (hbaseTemplate.tableExists(hbaseMapping.getHbaseTable())) {
                    hbaseTemplate.disableTable(hbaseMapping.getHbaseTable());
                    hbaseTemplate.deleteTable(hbaseMapping.getHbaseTable());
                }
                params = null;
            } else {
                logger.info(hbaseMapping.getHbaseTable() + " etl is starting!");
            }
            createTable(hbaseTemplate, config);

            // 拼接sql
            String sql = "SELECT * FROM " + config.getHbaseMapping().getDatabase() + "." + hbaseMapping.getTable();

            // 拼接条件
            if (params != null && params.size() == 1 && hbaseMapping.getEtlCondition() == null) {
                AtomicBoolean stExists = new AtomicBoolean(false);
                // 验证是否有SYS_TIME字段
                Util.sqlRS(ds, sql, rs -> {
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
                    sql += " WHERE SYS_TIME >= '" + params.get(0) + "' ";
                }
            } else if (hbaseMapping.getEtlCondition() != null && params != null) {
                String etlCondition = hbaseMapping.getEtlCondition();
                int size = params.size();
                for (int i = 0; i < size; i++) {
                    etlCondition = etlCondition.replace("{" + i + "}", params.get(i));
                }

                sql += " " + etlCondition;
            }

            // 获取总数
            String countSql = "SELECT COUNT(1) FROM ( " + sql + ") _CNT ";
            long cnt = (Long) Util.sqlRS(ds, countSql, rs -> {
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
                    executor.submit(
                        () -> executeSqlImport(ds, sqlFinal, hbaseMapping, hbaseTemplate, successCount, errMsg));
                }

                executor.shutdown();
                while (!executor.awaitTermination(3, TimeUnit.SECONDS)) {
                    // ignore
                }
            } else {
                executeSqlImport(ds, sql, hbaseMapping, hbaseTemplate, successCount, errMsg);
            }

            logger.info(hbaseMapping.getHbaseTable() + " etl completed in: "
                        + (System.currentTimeMillis() - start) / 1000 + "s!");

            etlResult.setResultMessage("导入HBase表 " + hbaseMapping.getHbaseTable() + " 数据：" + successCount.get() + " 条");
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

    /**
     * 执行导入
     *
     * @param ds
     * @param sql
     * @param hbaseMapping
     * @param hbaseTemplate
     * @param successCount
     * @param errMsg
     * @return
     */
    private static boolean executeSqlImport(DataSource ds, String sql, MappingConfig.HbaseMapping hbaseMapping,
                                            HbaseTemplate hbaseTemplate, AtomicLong successCount, List<String> errMsg) {
        try {
            Util.sqlRS(ds, sql, rs -> {
                int i = 1;

                try {
                    boolean complete = false;
                    List<HRow> rows = new ArrayList<>();
                    String[] rowKeyColumns = null;
                    if (hbaseMapping.getRowKey() != null) {
                        rowKeyColumns = hbaseMapping.getRowKey().trim().split(",");
                    }
                    while (rs.next()) {
                        int cc = rs.getMetaData().getColumnCount();
                        int[] jdbcTypes = new int[cc];
                        Class<?>[] classes = new Class[cc];
                        for (int j = 1; j <= cc; j++) {
                            int jdbcType = rs.getMetaData().getColumnType(j);
                            jdbcTypes[j - 1] = jdbcType;
                            classes[j - 1] = JdbcTypeUtil.jdbcType2javaType(jdbcType);
                        }
                        HRow row = new HRow();

                        if (rowKeyColumns != null) {
                            // 取rowKey字段拼接
                            StringBuilder rowKeyVale = new StringBuilder();
                            for (String rowKeyColumnName : rowKeyColumns) {
                                Object obj = rs.getObject(rowKeyColumnName);
                                if (obj != null) {
                                    rowKeyVale.append(obj.toString());
                                }
                                rowKeyVale.append("|");
                            }
                            int len = rowKeyVale.length();
                            if (len > 0) {
                                rowKeyVale.delete(len - 1, len);
                            }
                            row.setRowKey(Bytes.toBytes(rowKeyVale.toString()));
                        }

                        for (int j = 1; j <= cc; j++) {
                            String columnName = rs.getMetaData().getColumnName(j);

                            Object val = JdbcTypeUtil.getRSData(rs, columnName, jdbcTypes[j - 1]);
                            if (val == null) {
                                continue;
                            }

                            MappingConfig.ColumnItem columnItem = hbaseMapping.getColumnItems().get(columnName);
                            // 没有配置映射
                            if (columnItem == null) {
                                String family = hbaseMapping.getFamily();
                                String qualifile = columnName;
                                if (hbaseMapping.isUppercaseQualifier()) {
                                    qualifile = qualifile.toUpperCase();
                                }
                                if (MappingConfig.Mode.STRING == hbaseMapping.getMode()) {
                                    if (hbaseMapping.getRowKey() == null && j == 1) {
                                        row.setRowKey(Bytes.toBytes(val.toString()));
                                    } else {
                                        row.addCell(family, qualifile, Bytes.toBytes(val.toString()));
                                    }
                                } else if (MappingConfig.Mode.NATIVE == hbaseMapping.getMode()) {
                                    Type type = Type.getType(classes[j - 1]);
                                    if (hbaseMapping.getRowKey() == null && j == 1) {
                                        row.setRowKey(TypeUtil.toBytes(val, type));
                                    } else {
                                        row.addCell(family, qualifile, TypeUtil.toBytes(val, type));
                                    }
                                } else if (MappingConfig.Mode.PHOENIX == hbaseMapping.getMode()) {
                                    PhType phType = PhType.getType(classes[j - 1]);
                                    if (hbaseMapping.getRowKey() == null && j == 1) {
                                        row.setRowKey(PhTypeUtil.toBytes(val, phType));
                                    } else {
                                        row.addCell(family, qualifile, PhTypeUtil.toBytes(val, phType));
                                    }
                                }
                            } else {
                                // 如果不需要类型转换
                                if (columnItem.getType() == null || "".equals(columnItem.getType())) {
                                    if (val instanceof java.sql.Date) {
                                        SimpleDateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd");
                                        val = dateFmt.format((Date) val);
                                    } else if (val instanceof Timestamp) {
                                        SimpleDateFormat datetimeFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                        val = datetimeFmt.format((Date) val);
                                    }

                                    byte[] valBytes = Bytes.toBytes(val.toString());
                                    if (columnItem.isRowKey()) {
                                        if (columnItem.getRowKeyLen() != null) {
                                            valBytes = Bytes.toBytes(limitLenNum(columnItem.getRowKeyLen(), val));
                                            row.setRowKey(valBytes);
                                        } else {
                                            row.setRowKey(valBytes);
                                        }
                                    } else {
                                        row.addCell(columnItem.getFamily(), columnItem.getQualifier(), valBytes);
                                    }
                                } else {
                                    if (MappingConfig.Mode.STRING == hbaseMapping.getMode()) {
                                        byte[] valBytes = Bytes.toBytes(val.toString());
                                        if (columnItem.isRowKey()) {
                                            if (columnItem.getRowKeyLen() != null) {
                                                valBytes = Bytes.toBytes(limitLenNum(columnItem.getRowKeyLen(), val));
                                            }
                                            row.setRowKey(valBytes);
                                        } else {
                                            row.addCell(columnItem.getFamily(), columnItem.getQualifier(), valBytes);
                                        }
                                    } else if (MappingConfig.Mode.NATIVE == hbaseMapping.getMode()) {
                                        Type type = Type.getType(columnItem.getType());
                                        if (columnItem.isRowKey()) {
                                            if (columnItem.getRowKeyLen() != null) {
                                                String v = limitLenNum(columnItem.getRowKeyLen(), val);
                                                row.setRowKey(Bytes.toBytes(v));
                                            } else {
                                                row.setRowKey(TypeUtil.toBytes(val, type));
                                            }
                                        } else {
                                            row.addCell(columnItem.getFamily(),
                                                columnItem.getQualifier(),
                                                TypeUtil.toBytes(val, type));
                                        }
                                    } else if (MappingConfig.Mode.PHOENIX == hbaseMapping.getMode()) {
                                        PhType phType = PhType.getType(columnItem.getType());
                                        if (columnItem.isRowKey()) {
                                            row.setRowKey(PhTypeUtil.toBytes(val, phType));
                                        } else {
                                            row.addCell(columnItem.getFamily(),
                                                columnItem.getQualifier(),
                                                PhTypeUtil.toBytes(val, phType));
                                        }
                                    }
                                }
                            }
                        }

                        if (row.getRowKey() == null) throw new RuntimeException("RowKey 值为空");

                        rows.add(row);
                        complete = false;
                        if (i % hbaseMapping.getCommitBatch() == 0 && !rows.isEmpty()) {
                            hbaseTemplate.puts(hbaseMapping.getHbaseTable(), rows);
                            rows.clear();
                            complete = true;
                        }
                        i++;
                        successCount.incrementAndGet();
                        if (logger.isDebugEnabled()) {
                            logger.debug("successful import count:" + successCount.get());
                        }
                    }

                    if (!complete && !rows.isEmpty()) {
                        hbaseTemplate.puts(hbaseMapping.getHbaseTable(), rows);
                    }

                } catch (Exception e) {
                    logger.error(hbaseMapping.getHbaseTable() + " etl failed! ==>" + e.getMessage(), e);
                    errMsg.add(hbaseMapping.getHbaseTable() + " etl failed! ==>" + e.getMessage());
                    // throw new RuntimeException(e);
                }
                return i;
            });
            return true;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        }
    }

    private static String limitLenNum(int len, Object val) {
        if (val == null) {
            return null;
        }
        if (val instanceof Number) {
            return String.format("%0" + len + "d", (Number) ((Number) val).longValue());
        } else if (val instanceof String) {
            return String.format("%0" + len + "d", Long.parseLong((String) val));
        }
        return null;
    }
}
