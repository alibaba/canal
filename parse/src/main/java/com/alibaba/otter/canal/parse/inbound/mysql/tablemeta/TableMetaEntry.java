package com.alibaba.otter.canal.parse.inbound.mysql.tablemeta;

import java.io.Serializable;

public class TableMetaEntry implements Serializable {

    private static final long serialVersionUID = -1350200637109107904L;

    private String dbAddress;
    private String schema;
    private String table;
    private String ddl;
    private Long timestamp;


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

    public String getDdl() {
        return ddl;
    }

    public void setDdl(String ddl) {
        this.ddl = ddl;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getDbAddress() {
        return dbAddress;
    }

    public void setDbAddress(String dbAddress) {
        this.dbAddress = dbAddress;
    }
}
