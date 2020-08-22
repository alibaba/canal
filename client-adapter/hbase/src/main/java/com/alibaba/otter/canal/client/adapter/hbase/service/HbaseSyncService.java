package com.alibaba.otter.canal.client.adapter.hbase.service;

import java.util.*;

import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.client.adapter.hbase.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.hbase.support.*;
import com.alibaba.otter.canal.client.adapter.support.Dml;

/**
 * HBase同步操作业务
 *
 * @author rewerma 2018-8-21 下午06:45:49
 * @version 1.0.0
 */
public class HbaseSyncService {

    private static Logger logger = LoggerFactory.getLogger(HbaseSyncService.class);

    private HbaseTemplate hbaseTemplate;                                           // HBase操作模板

    public HbaseSyncService(HbaseTemplate hbaseTemplate){
        this.hbaseTemplate = hbaseTemplate;
    }

    public void sync(MappingConfig config, Dml dml) {
        if (config != null) {
            String type = dml.getType();
            if (type != null && type.equalsIgnoreCase("INSERT")) {
                insert(config, dml);
            } else if (type != null && type.equalsIgnoreCase("UPDATE")) {
                update(config, dml);
            } else if (type != null && type.equalsIgnoreCase("DELETE")) {
                delete(config, dml);
            }
            if (logger.isDebugEnabled()) {
                logger.debug("DML: {}", JSON.toJSONString(dml, SerializerFeature.WriteMapNullValue));
            }
        }
    }

    /**
     * 插入操作
     *
     * @param config 配置项
     * @param dml DML数据
     */
    private void insert(MappingConfig config, Dml dml) {
        List<Map<String, Object>> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }

        MappingConfig.HbaseMapping hbaseMapping = config.getHbaseMapping();

        // if (!validHTable(config)) {
        // logger.error("HBase table '{}' not exists",
        // hbaseMapping.getHbaseTable());
        // return;
        // }
        int i = 1;
        boolean complete = false;
        List<HRow> rows = new ArrayList<>();
        for (Map<String, Object> r : data) {
            HRow hRow = new HRow();

            // 拼接复合rowKey
            if (hbaseMapping.getRowKey() != null) {
                String[] rowKeyColumns = hbaseMapping.getRowKey().trim().split(",");
                String rowKeyVale = getRowKeys(rowKeyColumns, r);
                // params.put("rowKey", Bytes.toBytes(rowKeyVale));
                hRow.setRowKey(Bytes.toBytes(rowKeyVale));
            }

            convertData2Row(hbaseMapping, hRow, r);
            if (hRow.getRowKey() == null) {
                throw new RuntimeException("empty rowKey");
            }
            rows.add(hRow);
            complete = false;
            if (i % config.getHbaseMapping().getCommitBatch() == 0 && !rows.isEmpty()) {
                hbaseTemplate.puts(hbaseMapping.getHbaseTable(), rows);
                rows.clear();
                complete = true;
            }
            i++;
        }
        if (!complete && !rows.isEmpty()) {
            hbaseTemplate.puts(hbaseMapping.getHbaseTable(), rows);
        }

    }

    /**
     * 将Map数据转换为HRow行数据
     *
     * @param hbaseMapping hbase映射配置
     * @param hRow 行对象
     * @param data Map数据
     */
    private static void convertData2Row(MappingConfig.HbaseMapping hbaseMapping, HRow hRow, Map<String, Object> data) {
        Map<String, MappingConfig.ColumnItem> columnItems = hbaseMapping.getColumnItems();
        int i = 0;
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            if (hbaseMapping.getExcludeColumns() != null && hbaseMapping.getExcludeColumns().contains(entry.getKey())) {
                continue;
            }
            if (entry.getValue() != null) {
                MappingConfig.ColumnItem columnItem = columnItems.get(entry.getKey());

                byte[] bytes = typeConvert(columnItem, hbaseMapping, entry.getValue());

                if (columnItem == null) {
                    String familyName = hbaseMapping.getFamily();
                    String qualifier = entry.getKey();
                    if (hbaseMapping.isUppercaseQualifier()) {
                        qualifier = qualifier.toUpperCase();
                    }

                    if (hbaseMapping.getRowKey() == null && i == 0) {
                        hRow.setRowKey(bytes);
                    } else {
                        hRow.addCell(familyName, qualifier, bytes);
                    }
                } else {
                    if (columnItem.isRowKey()) {
                        if (columnItem.getRowKeyLen() != null && entry.getValue() != null) {
                            if (entry.getValue() instanceof Number) {
                                String v = String.format("%0" + columnItem.getRowKeyLen() + "d",
                                    ((Number) entry.getValue()).longValue());
                                bytes = Bytes.toBytes(v);
                            } else {
                                try {
                                    String v = String.format("%0" + columnItem.getRowKeyLen() + "d",
                                        Integer.parseInt((String) entry.getValue()));
                                    bytes = Bytes.toBytes(v);
                                } catch (Exception e) {
                                    logger.error(e.getMessage(), e);
                                }
                            }
                        }
                        hRow.setRowKey(bytes);
                    } else {
                        hRow.addCell(columnItem.getFamily(), columnItem.getQualifier(), bytes);
                    }
                }
            }
            i++;
        }
    }

    /**
     * 更新操作
     *
     * @param config 配置对象
     * @param dml dml对象
     */
    private void update(MappingConfig config, Dml dml) {
        List<Map<String, Object>> data = dml.getData();
        List<Map<String, Object>> old = dml.getOld();
        if (old == null || old.isEmpty() || data == null || data.isEmpty()) {
            return;
        }

        MappingConfig.HbaseMapping hbaseMapping = config.getHbaseMapping();

        // if (!validHTable(config)) {
        // logger.error("HBase table '{}' not exists",
        // hbaseMapping.getHbaseTable());
        // return;
        // }

        MappingConfig.ColumnItem rowKeyColumn = hbaseMapping.getRowKeyColumn();
        int index = 0;
        int i = 1;
        boolean complete = false;
        List<HRow> rows = new ArrayList<>();
        out: for (Map<String, Object> r : data) {
            byte[] rowKeyBytes;

            if (hbaseMapping.getRowKey() != null) {
                String[] rowKeyColumns = hbaseMapping.getRowKey().trim().split(",");

                // 判断是否有复合主键修改
                for (String updateColumn : old.get(index).keySet()) {
                    for (String rowKeyColumnName : rowKeyColumns) {
                        if (rowKeyColumnName.equalsIgnoreCase(updateColumn)) {
                            // 调用删除插入操作
                            deleteAndInsert(config, dml);
                            continue out;
                        }
                    }
                }

                String rowKeyVale = getRowKeys(rowKeyColumns, r);
                rowKeyBytes = Bytes.toBytes(rowKeyVale);
            } else if (rowKeyColumn == null) {
                rowKeyBytes = typeConvert(null, hbaseMapping, r.values().iterator().next());
            } else {
                rowKeyBytes = getRowKeyBytes(hbaseMapping, rowKeyColumn, r);
            }
            if (rowKeyBytes == null) throw new RuntimeException("rowKey值为空");

            Map<String, MappingConfig.ColumnItem> columnItems = hbaseMapping.getColumnItems();
            HRow hRow = new HRow(rowKeyBytes);
            for (String updateColumn : old.get(index).keySet()) {
                if (hbaseMapping.getExcludeColumns() != null
                    && hbaseMapping.getExcludeColumns().contains(updateColumn)) {
                    continue;
                }
                MappingConfig.ColumnItem columnItem = columnItems.get(updateColumn);
                if (columnItem == null) {
                    String family = hbaseMapping.getFamily();
                    String qualifier = updateColumn;
                    if (hbaseMapping.isUppercaseQualifier()) {
                        qualifier = qualifier.toUpperCase();
                    }

                    Object newVal = r.get(updateColumn);

                    if (newVal == null) {
                        hRow.addCell(family, qualifier, null);
                    } else {
                        hRow.addCell(family, qualifier, typeConvert(null, hbaseMapping, newVal));
                    }
                } else {
                    // 排除修改id的情况
                    if (columnItem.isRowKey()) continue;

                    Object newVal = r.get(updateColumn);
                    if (newVal == null) {
                        hRow.addCell(columnItem.getFamily(), columnItem.getQualifier(), null);
                    } else {
                        hRow.addCell(columnItem.getFamily(),
                            columnItem.getQualifier(),
                            typeConvert(columnItem, hbaseMapping, newVal));
                    }
                }
            }
            rows.add(hRow);
            complete = false;
            if (i % config.getHbaseMapping().getCommitBatch() == 0 && !rows.isEmpty()) {
                hbaseTemplate.puts(hbaseMapping.getHbaseTable(), rows);
                rows.clear();
                complete = true;
            }
            i++;
            index++;
        }
        if (!complete && !rows.isEmpty()) {
            hbaseTemplate.puts(hbaseMapping.getHbaseTable(), rows);
        }
    }

    private void delete(MappingConfig config, Dml dml) {
        List<Map<String, Object>> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }

        MappingConfig.HbaseMapping hbaseMapping = config.getHbaseMapping();

        // if (!validHTable(config)) {
        // logger.error("HBase table '{}' not exists",
        // hbaseMapping.getHbaseTable());
        // return;
        // }

        MappingConfig.ColumnItem rowKeyColumn = hbaseMapping.getRowKeyColumn();
        boolean complete = false;
        int i = 1;
        Set<byte[]> rowKeys = new HashSet<>();
        for (Map<String, Object> r : data) {
            byte[] rowKeyBytes;

            if (hbaseMapping.getRowKey() != null) {
                String[] rowKeyColumns = hbaseMapping.getRowKey().trim().split(",");
                String rowKeyVale = getRowKeys(rowKeyColumns, r);
                rowKeyBytes = Bytes.toBytes(rowKeyVale);
            } else if (rowKeyColumn == null) {
                // 如果不需要类型转换
                rowKeyBytes = typeConvert(null, hbaseMapping, r.values().iterator().next());
            } else {
                rowKeyBytes = getRowKeyBytes(hbaseMapping, rowKeyColumn, r);
            }
            if (rowKeyBytes == null) throw new RuntimeException("rowKey值为空");
            rowKeys.add(rowKeyBytes);
            complete = false;
            if (i % config.getHbaseMapping().getCommitBatch() == 0 && !rowKeys.isEmpty()) {
                hbaseTemplate.deletes(hbaseMapping.getHbaseTable(), rowKeys);
                rowKeys.clear();
                complete = true;
            }
            i++;
        }
        if (!complete && !rowKeys.isEmpty()) {
            hbaseTemplate.deletes(hbaseMapping.getHbaseTable(), rowKeys);
        }
    }

    private void deleteAndInsert(MappingConfig config, Dml dml) {
        List<Map<String, Object>> data = dml.getData();
        List<Map<String, Object>> old = dml.getOld();
        if (old == null || old.isEmpty() || data == null || data.isEmpty()) {
            return;
        }
        MappingConfig.HbaseMapping hbaseMapping = config.getHbaseMapping();

        String[] rowKeyColumns = hbaseMapping.getRowKey().trim().split(",");

        int index = 0;
        int i = 1;
        boolean complete = false;
        Set<byte[]> rowKeys = new HashSet<>();
        List<HRow> rows = new ArrayList<>();
        for (Map<String, Object> r : data) {
            // 拼接老的rowKey
            List<String> updateSubRowKey = new ArrayList<>();
            for (String rowKeyColumnName : rowKeyColumns) {
                for (String updateColumn : old.get(index).keySet()) {
                    if (rowKeyColumnName.equalsIgnoreCase(updateColumn)) {
                        updateSubRowKey.add(rowKeyColumnName);
                    }
                }
            }
            if (updateSubRowKey.isEmpty()) {
                throw new RuntimeException("没有更新复合主键的RowKey");
            }
            StringBuilder oldRowKey = new StringBuilder();
            StringBuilder newRowKey = new StringBuilder();
            for (String rowKeyColumnName : rowKeyColumns) {
                newRowKey.append(r.get(rowKeyColumnName).toString()).append("|");
                if (!updateSubRowKey.contains(rowKeyColumnName)) {
                    // 从data取
                    oldRowKey.append(r.get(rowKeyColumnName).toString()).append("|");
                } else {
                    // 从old取
                    oldRowKey.append(old.get(index).get(rowKeyColumnName).toString()).append("|");
                }
            }
            int len = newRowKey.length();
            newRowKey.delete(len - 1, len);
            len = oldRowKey.length();
            oldRowKey.delete(len - 1, len);
            byte[] newRowKeyBytes = Bytes.toBytes(newRowKey.toString());
            byte[] oldRowKeyBytes = Bytes.toBytes(oldRowKey.toString());

            rowKeys.add(oldRowKeyBytes);
            HRow row = new HRow(newRowKeyBytes);
            convertData2Row(hbaseMapping, row, r);
            rows.add(row);
            complete = false;
            if (i % config.getHbaseMapping().getCommitBatch() == 0 && !rows.isEmpty()) {
                hbaseTemplate.deletes(hbaseMapping.getHbaseTable(), rowKeys);

                hbaseTemplate.puts(hbaseMapping.getHbaseTable(), rows);
                rowKeys.clear();
                rows.clear();
                complete = true;
            }
            i++;
            index++;
        }
        if (!complete && !rows.isEmpty()) {
            hbaseTemplate.deletes(hbaseMapping.getHbaseTable(), rowKeys);
            hbaseTemplate.puts(hbaseMapping.getHbaseTable(), rows);
        }
    }

    /**
     * 根据对应的类型进行转换
     *
     * @param columnItem 列项配置
     * @param hbaseMapping hbase映射配置
     * @param value 值
     * @return 复合字段rowKey
     */
    private static byte[] typeConvert(MappingConfig.ColumnItem columnItem, MappingConfig.HbaseMapping hbaseMapping,
                                      Object value) {
        if (value == null) {
            return null;
        }
        byte[] bytes = null;
        if (columnItem == null || columnItem.getType() == null || "".equals(columnItem.getType())) {
            if (MappingConfig.Mode.STRING == hbaseMapping.getMode()) {
                bytes = Bytes.toBytes(value.toString());
            } else if (MappingConfig.Mode.NATIVE == hbaseMapping.getMode()) {
                bytes = TypeUtil.toBytes(value);
            } else if (MappingConfig.Mode.PHOENIX == hbaseMapping.getMode()) {
                PhType phType = PhType.getType(value.getClass());
                bytes = PhTypeUtil.toBytes(value, phType);
            }
        } else {
            if (hbaseMapping.getMode() == MappingConfig.Mode.STRING) {
                bytes = Bytes.toBytes(value.toString());
            } else if (hbaseMapping.getMode() == MappingConfig.Mode.NATIVE) {
                Type type = Type.getType(columnItem.getType());
                bytes = TypeUtil.toBytes(value, type);
            } else if (hbaseMapping.getMode() == MappingConfig.Mode.PHOENIX) {
                PhType phType = PhType.getType(columnItem.getType());
                bytes = PhTypeUtil.toBytes(value, phType);
            }
        }
        return bytes;
    }

    /**
     * 获取复合字段作为rowKey的拼接
     *
     * @param rowKeyColumns 复合rowK对应的字段
     * @param data 数据
     * @return
     */
    private static String getRowKeys(String[] rowKeyColumns, Map<String, Object> data) {
        StringBuilder rowKeyValue = new StringBuilder();
        for (String rowKeyColumnName : rowKeyColumns) {
            Object obj = data.get(rowKeyColumnName);
            if (obj != null) {
                rowKeyValue.append(obj.toString());
            }
            rowKeyValue.append("|");
        }
        int len = rowKeyValue.length();
        if (len > 0) {
            rowKeyValue.delete(len - 1, len);
        }
        return rowKeyValue.toString();
    }

    private static byte[] getRowKeyBytes(MappingConfig.HbaseMapping hbaseMapping, MappingConfig.ColumnItem rowKeyColumn,
                                         Map<String, Object> rowData) {
        Object val = rowData.get(rowKeyColumn.getColumn());
        String v = null;
        if (rowKeyColumn.getRowKeyLen() != null) {
            if (val instanceof Number) {
                v = String.format("%0" + rowKeyColumn.getRowKeyLen() + "d", (Number) ((Number) val).longValue());
            } else if (val instanceof String) {
                v = String.format("%0" + rowKeyColumn.getRowKeyLen() + "d", Long.parseLong((String) val));
            }
        }
        if (v != null) {
            return Bytes.toBytes(v);
        } else {
            return typeConvert(rowKeyColumn, hbaseMapping, val);
        }
    }

}
