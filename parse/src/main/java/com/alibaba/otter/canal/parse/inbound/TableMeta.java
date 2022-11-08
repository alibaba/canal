package com.alibaba.otter.canal.parse.inbound;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.dbsync.binlog.event.TableMapLogEvent;

/**
 * 描述数据meta对象,mysql binlog中对应的{@linkplain TableMapLogEvent}包含的信息不全
 *
 * <pre>
 * 1. 主键信息
 * 2. column name
 * 3. unsigned字段
 * </pre>
 *
 * @author jianghang 2013-1-18 下午12:24:59
 * @version 1.0.0
 */
public class TableMeta {

    private String          schema;
    private String          table;
    private List<FieldMeta> fields = new ArrayList<>();
    private String          ddl;                                          // 表结构的DDL语句

    public TableMeta(){

    }

    public TableMeta(String schema, String table, List<FieldMeta> fields){
        this.schema = schema;
        this.table = table;
        this.fields = fields;
    }

    public String getFullName() {
        return schema + "." + table;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<FieldMeta> getFields() {
        return fields;
    }

    public void setFields(List<FieldMeta> fileds) {
        this.fields = fileds;
    }

    public FieldMeta getFieldMetaByName(String name) {
        for (FieldMeta meta : fields) {
            if (meta.getColumnName().equalsIgnoreCase(name)) {
                return meta;
            }
        }

        throw new RuntimeException("unknow column : " + name);
    }

    public List<FieldMeta> getPrimaryFields() {
        List<FieldMeta> primarys = new ArrayList<>();
        for (FieldMeta meta : fields) {
            if (meta.isKey()) {
                primarys.add(meta);
            }
        }

        return primarys;
    }

    public String getDdl() {
        return ddl;
    }

    public void setDdl(String ddl) {
        this.ddl = ddl;
    }

    public void addFieldMeta(FieldMeta fieldMeta) {
        this.fields.add(fieldMeta);
    }

    @Override
    public String toString() {
        StringBuilder data = new StringBuilder();
        data.append("TableMeta [schema=" + schema + ", table=" + table + ", fileds=");
        for (FieldMeta field : fields) {
            data.append("\n\t").append(field.toString());
        }
        data.append("\n]");
        return data.toString();
    }

    public static class FieldMeta {

        public FieldMeta(){

        }

        public FieldMeta(String columnName, String columnType, boolean nullable, boolean key, String defaultValue){
            this.columnName = columnName;
            this.columnType = columnType;
            this.nullable = nullable;
            this.key = key;
            this.defaultValue = defaultValue;
        }

        private String  columnName;
        private String  columnType;
        private boolean nullable;
        private boolean key;
        private String  defaultValue;
        private String  extra;
        private boolean unique;

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public String getColumnType() {
            return columnType;
        }

        public void setColumnType(String columnType) {
            this.columnType = columnType;
        }

        public void setNullable(boolean nullable) {
            this.nullable = nullable;
        }

        public String getDefaultValue() {
            return defaultValue;
        }

        public void setDefaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
        }

        public boolean isUnsigned() {
            return StringUtils.containsIgnoreCase(columnType, "unsigned");
        }

        public boolean isNullable() {
            return nullable;
        }

        public boolean isKey() {
            return key;
        }

        public void setKey(boolean key) {
            this.key = key;
        }

        public String getExtra() {
            return extra;
        }

        public void setExtra(String extra) {
            this.extra = extra;
        }

        public boolean isUnique() {
            return unique;
        }

        public void setUnique(boolean unique) {
            this.unique = unique;
        }

        @Override
        public String toString() {
            return "FieldMeta [columnName=" + columnName + ", columnType=" + columnType + ", nullable=" + nullable
                   + ", key=" + key + ", defaultValue=" + defaultValue + ", extra=" + extra + ", unique=" + unique
                   + "]";
        }

    }

}
