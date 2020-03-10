package com.alibaba.otter.canal.client.adapter.hbase.service;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.sql.DataSource;

import org.apache.hadoop.hbase.util.Bytes;

import com.alibaba.otter.canal.client.adapter.hbase.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.hbase.support.HRow;
import com.alibaba.otter.canal.client.adapter.hbase.support.HbaseTemplate;
import com.alibaba.otter.canal.client.adapter.hbase.support.PhType;
import com.alibaba.otter.canal.client.adapter.hbase.support.PhTypeUtil;
import com.alibaba.otter.canal.client.adapter.hbase.support.Type;
import com.alibaba.otter.canal.client.adapter.hbase.support.TypeUtil;
import com.alibaba.otter.canal.client.adapter.support.AbstractEtlService;
import com.alibaba.otter.canal.client.adapter.support.AdapterConfig;
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
public class HbaseEtlService extends AbstractEtlService {

    private HbaseTemplate hbaseTemplate;
    private MappingConfig config;

    public HbaseEtlService(HbaseTemplate hbaseTemplate, MappingConfig config){
        super("HBase", config);
        this.hbaseTemplate = hbaseTemplate;
        this.config = config;
    }

    /**
     * 建表
     */
    private void createTable() {
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
     * @param params 筛选条件
     * @return 导入结果
     */
    public EtlResult importData(List<String> params) {
        EtlResult etlResult = new EtlResult();
        List<String> errMsg = new ArrayList<>();
        try {
            MappingConfig.HbaseMapping hbaseMapping = config.getHbaseMapping();

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
            createTable();

            // 拼接sql
            String sql = "SELECT * FROM `" + config.getHbaseMapping().getDatabase() + "`.`" + hbaseMapping.getTable()
                         + "`";

            return super.importData(sql, params);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            errMsg.add("HBase etl error ==>" + e.getMessage());
        }
        etlResult.setErrorMessage(Joiner.on("\n").join(errMsg));
        return etlResult;
    }

    /**
     * 执行导入
     */
    protected boolean executeSqlImport(DataSource ds, String sql, List<Object> values,
                                       AdapterConfig.AdapterMapping mapping, AtomicLong impCount, List<String> errMsg) {
        MappingConfig.HbaseMapping hbaseMapping = (MappingConfig.HbaseMapping) mapping;
        try {
            Util.sqlRS(ds, sql, values, rs -> {
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
            impCount.incrementAndGet();
            if (logger.isDebugEnabled()) {
                logger.debug("successful import count:" + impCount.get());
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
}           );
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
