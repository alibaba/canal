package com.alibaba.otter.canal.client.adapter.hbase.config;

import com.alibaba.otter.canal.client.adapter.support.AdapterConfig;

import java.util.*;

/**
 * HBase表映射配置
 *
 * @author rewerma 2018-8-21 下午06:45:49
 * @version 1.0.0
 */
public class MappingConfig implements AdapterConfig {

    private String       dataSourceKey;   // 数据源key

    private String       outerAdapterKey; // adapter key

    private String       groupId;         // groupId

    private String       destination;     // canal实例或MQ的topic

    private HbaseMapping hbaseMapping;    // hbase映射配置

    public String getDataSourceKey() {
        return dataSourceKey;
    }

    public void setDataSourceKey(String dataSourceKey) {
        this.dataSourceKey = dataSourceKey;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getOuterAdapterKey() {
        return outerAdapterKey;
    }

    public void setOuterAdapterKey(String outerAdapterKey) {
        this.outerAdapterKey = outerAdapterKey;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public HbaseMapping getHbaseMapping() {
        return hbaseMapping;
    }

    public void setHbaseMapping(HbaseMapping hbaseMapping) {
        this.hbaseMapping = hbaseMapping;
    }

    public AdapterMapping getMapping() {
        return hbaseMapping;
    }

    public void validate() {
        if (hbaseMapping.database == null || hbaseMapping.database.isEmpty()) {
            throw new NullPointerException("hbaseMapping.database");
        }
        if (hbaseMapping.table == null || hbaseMapping.table.isEmpty()) {
            throw new NullPointerException("hbaseMapping.table");
        }
        if (hbaseMapping.hbaseTable == null || hbaseMapping.hbaseTable.isEmpty()) {
            throw new NullPointerException("hbaseMapping.hbaseTable");
        }
        if (hbaseMapping.mode == null) {
            throw new NullPointerException("hbaseMapping.mode");
        }
        if (hbaseMapping.rowKey != null && hbaseMapping.rowKeyColumn != null) {
            throw new RuntimeException("已配置了复合主键作为RowKey，无需再指定RowKey列");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MappingConfig config = (MappingConfig) o;

        return hbaseMapping != null ? hbaseMapping.equals(config.hbaseMapping) : config.hbaseMapping == null;
    }

    @Override
    public int hashCode() {
        return hbaseMapping != null ? hbaseMapping.hashCode() : 0;
    }

    public static class ColumnItem {

        private boolean isRowKey = false;
        private Integer rowKeyLen;
        private String  column;
        private String  family;
        private String  qualifier;
        private String  type;

        public boolean isRowKey() {
            return isRowKey;
        }

        public void setRowKey(boolean rowKey) {
            isRowKey = rowKey;
        }

        public Integer getRowKeyLen() {
            return rowKeyLen;
        }

        public void setRowKeyLen(Integer rowKeyLen) {
            this.rowKeyLen = rowKeyLen;
        }

        public String getColumn() {
            return column;
        }

        public void setColumn(String column) {
            this.column = column;
        }

        public String getFamily() {
            return family;
        }

        public void setFamily(String family) {
            this.family = family;
        }

        public String getQualifier() {
            return qualifier;
        }

        public void setQualifier(String qualifier) {
            this.qualifier = qualifier;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ColumnItem that = (ColumnItem) o;
            return Objects.equals(column, that.column);
        }

        @Override
        public int hashCode() {

            return Objects.hash(column);
        }
    }

    public enum Mode {
                      STRING("STRING"), NATIVE("NATIVE"), PHOENIX("PHOENIX");

        private String type;

        public String getType() {
            return type;
        }

        Mode(String type){
            this.type = type;
        }
    }

    public static class HbaseMapping implements AdapterMapping {

        private Mode                    mode               = Mode.STRING;           // hbase默认转换格式
        private String                  database;                                   // 数据库名或schema名
        private String                  table;                                      // 表面名
        private String                  hbaseTable;                                 // hbase表名
        private String                  family             = "CF";                  // 默认统一column family
        private boolean                 uppercaseQualifier = true;                  // 是否转大写
        private boolean                 autoCreateTable    = false;                 // 同步时HBase中表不存在的情况下自动建表
        private String                  rowKey;                                     // 指定复合主键为rowKey
        private Map<String, String>     columns;                                    // 字段映射
        private List<String>            excludeColumns;                             // 不映射的字段
        private ColumnItem              rowKeyColumn;                               // rowKey字段
        private String                  etlCondition;                               // etl条件sql

        private Map<String, ColumnItem> columnItems        = new LinkedHashMap<>(); // 转换后的字段映射列表
        private Set<String>             families           = new LinkedHashSet<>(); // column family列表
        private int                     readBatch          = 5000;
        private int                     commitBatch        = 5000;                  // etl等批量提交大小

        public Mode getMode() {
            return mode;
        }

        public void setMode(Mode mode) {
            this.mode = mode;
        }

        public String getDatabase() {
            return database;
        }

        public void setDatabase(String database) {
            this.database = database;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public String getHbaseTable() {
            return hbaseTable;
        }

        public void setHbaseTable(String hbaseTable) {
            this.hbaseTable = hbaseTable;
        }

        public Map<String, String> getColumns() {
            return columns;
        }

        public boolean isAutoCreateTable() {
            return autoCreateTable;
        }

        public void setAutoCreateTable(boolean autoCreateTable) {
            this.autoCreateTable = autoCreateTable;
        }

        public int getReadBatch() {
            return readBatch;
        }

        public void setReadBatch(int readBatch) {
            this.readBatch = readBatch;
        }

        public int getCommitBatch() {
            return commitBatch;
        }

        public void setCommitBatch(int commitBatch) {
            this.commitBatch = commitBatch;
        }

        public String getRowKey() {
            return rowKey;
        }

        public void setRowKey(String rowKey) {
            this.rowKey = rowKey;
        }

        public String getEtlCondition() {
            return etlCondition;
        }

        public void setEtlCondition(String etlCondition) {
            this.etlCondition = etlCondition;
        }

        public void setColumns(Map<String, String> columns) {
            this.columns = columns;

            if (columns != null) {
                for (Map.Entry<String, String> columnField : columns.entrySet()) {
                    String field = columnField.getValue();
                    String type = null;
                    if (field != null) {
                        // 解析类型
                        int i = field.indexOf("$");
                        if (i > -1) {
                            type = field.substring(i + 1);
                            field = field.substring(0, i);
                        }
                    }
                    ColumnItem columnItem = new ColumnItem();
                    columnItem.setColumn(columnField.getKey());
                    columnItem.setType(type);
                    if (field != null && field.toUpperCase().startsWith("ROWKEY")) {
                        int idx = field.toUpperCase().indexOf("LEN:");
                        if (idx > -1) {
                            String len = field.substring(idx + 4);
                            try {
                                columnItem.setRowKeyLen(Integer.parseInt(len));
                            } catch (Exception e) {
                                // ignore
                            }
                        }
                        columnItem.setRowKey(true);
                        rowKeyColumn = columnItem;
                    } else {
                        if (field == null || field.equals("")) {
                            columnItem.setFamily(family);
                            columnItem.setQualifier(columnField.getKey());
                        } else {
                            int len = field.indexOf(":");
                            if (len > -1) {
                                columnItem.setFamily(field.substring(0, len));
                                columnItem.setQualifier(field.substring(len + 1));
                            } else {
                                columnItem.setFamily(family);
                                columnItem.setQualifier(field);
                            }
                        }
                        if (uppercaseQualifier) {
                            columnItem.setQualifier(columnItem.getQualifier().toUpperCase());
                        }
                        families.add(columnItem.getFamily());
                    }

                    columnItems.put(columnField.getKey(), columnItem);
                }
            } else {
                this.columns = new LinkedHashMap<>();
            }
        }

        public List<String> getExcludeColumns() {
            return excludeColumns;
        }

        public void setExcludeColumns(List<String> excludeColumns) {
            this.excludeColumns = excludeColumns;
        }

        public String getFamily() {
            return family;
        }

        public void setFamily(String family) {
            this.family = family;
            if (family == null) {
                this.family = "CF";
            }
        }

        public boolean isUppercaseQualifier() {
            return uppercaseQualifier;
        }

        public void setUppercaseQualifier(boolean uppercaseQualifier) {
            this.uppercaseQualifier = uppercaseQualifier;
        }

        public ColumnItem getRowKeyColumn() {
            return rowKeyColumn;
        }

        public void setRowKeyColumn(ColumnItem rowKeyColumn) {
            this.rowKeyColumn = rowKeyColumn;
        }

        public Map<String, ColumnItem> getColumnItems() {
            return columnItems;
        }

        public void setColumnItems(Map<String, ColumnItem> columnItems) {
            this.columnItems = columnItems;
        }

        public Set<String> getFamilies() {
            return families;
        }

        public void setFamilies(Set<String> families) {
            this.families = families;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            HbaseMapping hbaseMapping = (HbaseMapping) o;

            if (table != null ? !table.equals(hbaseMapping.table) : hbaseMapping.table != null) return false;
            return hbaseTable != null ? hbaseTable.equals(hbaseMapping.hbaseTable) : hbaseMapping.hbaseTable == null;
        }

        @Override
        public int hashCode() {
            int result = table != null ? table.hashCode() : 0;
            result = 31 * result + (hbaseTable != null ? hbaseTable.hashCode() : 0);
            return result;
        }
    }
}
