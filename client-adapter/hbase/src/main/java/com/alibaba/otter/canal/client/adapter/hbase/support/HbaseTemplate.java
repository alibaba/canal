package com.alibaba.otter.canal.client.adapter.hbase.support;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HBase操作模板
 *
 * @author machengyuan 2018-8-21 下午10:12:34
 * @version 1.0.0
 */
public class HbaseTemplate {

    private Logger     logger = LoggerFactory.getLogger(this.getClass());

    private Connection conn;

    public HbaseTemplate(Connection conn){
        this.conn = conn;
    }

    public boolean tableExists(String tableName) {
        try (HBaseAdmin admin = (HBaseAdmin) conn.getAdmin()) {

            return admin.tableExists(TableName.valueOf(tableName));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void createTable(String tableName, String... familyNames) {
        try (HBaseAdmin admin = (HBaseAdmin) conn.getAdmin()) {

            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
            // 添加列簇
            if (familyNames != null) {
                for (String familyName : familyNames) {
                    HColumnDescriptor hcd = new HColumnDescriptor(familyName);
                    desc.addFamily(hcd);
                }
            }
            admin.createTable(desc);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void disableTable(String tableName) {
        try (HBaseAdmin admin = (HBaseAdmin) conn.getAdmin()) {
            admin.disableTable(tableName);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public void deleteTable(String tableName) {
        try (HBaseAdmin admin = (HBaseAdmin) conn.getAdmin()) {
            if (admin.isTableEnabled(tableName)) {
                disableTable(tableName);
            }
            admin.deleteTable(tableName);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    /**
     * 插入一行数据
     * 
     * @param tableName 表名
     * @param hRow 行数据对象
     * @return 是否成功
     */
    public Boolean put(String tableName, HRow hRow) {
        boolean flag = false;
        try {
            HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
            Put put = new Put(hRow.getRowKey());
            for (HRow.HCell hCell : hRow.getCells()) {
                put.addColumn(Bytes.toBytes(hCell.getFamily()), Bytes.toBytes(hCell.getQualifier()), hCell.getValue());
            }
            table.put(put);
            flag = true;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return flag;

    }

    /**
     * 批量插入
     * 
     * @param tableName 表名
     * @param rows 行数据对象集合
     * @return 是否成功
     */
    public Boolean puts(String tableName, List<HRow> rows) {
        boolean flag = false;
        try {
            HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
            List<Put> puts = new ArrayList<>();
            for (HRow hRow : rows) {
                Put put = new Put(hRow.getRowKey());
                for (HRow.HCell hCell : hRow.getCells()) {
                    put.addColumn(Bytes.toBytes(hCell.getFamily()),
                        Bytes.toBytes(hCell.getQualifier()),
                        hCell.getValue());
                }
                puts.add(put);
            }
            if (!puts.isEmpty()) {
                table.put(puts);
            }
            flag = true;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return flag;
    }

    /**
     * 批量删除数据
     * 
     * @param tableName 表名
     * @param rowKeys rowKey集合
     * @return 是否成功
     */
    public Boolean deletes(String tableName, Set<byte[]> rowKeys) {
        boolean flag = false;
        try {
            HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
            List<Delete> deletes = new ArrayList<>();
            for (byte[] rowKey : rowKeys) {
                Delete delete = new Delete(rowKey);
                deletes.add(delete);
            }
            if (!deletes.isEmpty()) {
                table.delete(deletes);
            }
            flag = true;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return flag;
    }
}
