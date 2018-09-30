package com.alibaba.otter.canal.client.adapter.hbase.config;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * HBase表映射配置
 *
 * @author machengyuan 2018-8-21 下午06:45:49
 * @version 1.0.0
 */
public class MappingConfig {

    private HbaseOrm hbaseOrm;

    public HbaseOrm getHbaseOrm() {
        return hbaseOrm;
    }

    public void setHbaseOrm(HbaseOrm hbaseOrm) {
        this.hbaseOrm = hbaseOrm;
    }

    public void validate() {
        if (hbaseOrm.database == null || hbaseOrm.database.isEmpty()) {
            throw new NullPointerException("hbaseOrm.database");
        }
        if (hbaseOrm.table == null || hbaseOrm.table.isEmpty()) {
            throw new NullPointerException("hbaseOrm.table");
        }
        if (hbaseOrm.hbaseTable == null || hbaseOrm.hbaseTable.isEmpty()) {
            throw new NullPointerException("hbaseOrm.hbaseTable");
        }
        if (hbaseOrm.mode == null) {
            throw new NullPointerException("hbaseOrm.mode");
        }
        if (hbaseOrm.rowKey != null && hbaseOrm.rowKeyColumn != null) {
            throw new RuntimeException("已配置了复合主键作为RowKey，无需再指定RowKey列");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MappingConfig config = (MappingConfig) o;

        return hbaseOrm != null ? hbaseOrm.equals(config.hbaseOrm) : config.hbaseOrm == null;
    }

    @Override
    public int hashCode() {
        return hbaseOrm != null ? hbaseOrm.hashCode() : 0;
    }

    public static class ColumnItem {

        private boolean isRowKey = false;
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

    public static class HbaseOrm {

        private Mode                    mode               = Mode.STRING;
        private String                  database;
        private String                  table;
        private String                  hbaseTable;
        private String                  family             = "CF";
        private boolean                 uppercaseQualifier = true;
        private boolean                 autoCreateTable    = false;                // 同步时HBase中表不存在的情况下自动建表
        private String                  rowKey;                                    // 指定复合主键为rowKey
        private Map<String, String>     columns;
        private ColumnItem              rowKeyColumn;
        private String                  etlCondition;

        private Map<String, ColumnItem> columnItems        = new LinkedHashMap<>();
        private Set<String>             families           = new LinkedHashSet<>();
        private int                     readBatch          = 5000;
        private int                     commitBatch        = 5000;

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
                    if ("rowKey".equalsIgnoreCase(field)) {
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

            HbaseOrm hbaseOrm = (HbaseOrm) o;

            if (table != null ? !table.equals(hbaseOrm.table) : hbaseOrm.table != null) return false;
            return hbaseTable != null ? hbaseTable.equals(hbaseOrm.hbaseTable) : hbaseOrm.hbaseTable == null;
        }

        @Override
        public int hashCode() {
            int result = table != null ? table.hashCode() : 0;
            result = 31 * result + (hbaseTable != null ? hbaseTable.hashCode() : 0);
            return result;
        }
    }
}
