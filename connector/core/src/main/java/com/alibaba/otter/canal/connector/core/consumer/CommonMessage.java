package com.alibaba.otter.canal.connector.core.consumer;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class CommonMessage implements Serializable {

    private static final long         serialVersionUID = 2611556444074013268L;

    private String                    database;                               // 数据库或schema
    private String                    table;                                  // 表名
    private List<String>              pkNames;
    private Boolean                   isDdl;
    // 类型:INSERT/UPDATE/DELETE
    private String                    type;
    // binlog executeTime, 执行耗时
    private Long                      es;
    // dml build timeStamp, 同步时间
    private Long                      ts;
    // 执行的sql,dml sql为空
    private String                    sql;
    // 数据列表
    private List<Map<String, Object>> data;
    // 旧数据列表,用于update,size和data的size一一对应
    private List<Map<String, Object>> old;

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

    public List<String> getPkNames() {
        return pkNames;
    }

    public void setPkNames(List<String> pkNames) {
        this.pkNames = pkNames;
    }

    public Boolean getIsDdl() {
        return isDdl;
    }

    public void setIsDdl(Boolean isDdl) {
        this.isDdl = isDdl;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public List<Map<String, Object>> getData() {
        return data;
    }

    public void setData(List<Map<String, Object>> data) {
        this.data = data;
    }

    public List<Map<String, Object>> getOld() {
        return old;
    }

    public void setOld(List<Map<String, Object>> old) {
        this.old = old;
    }

    public Long getEs() {
        return es;
    }

    public void setEs(Long es) {
        this.es = es;
    }

    public void clear() {
        database = null;
        table = null;
        type = null;
        ts = null;
        es = null;
        data = null;
        old = null;
        sql = null;
    }

    @Override
    public String toString() {
        return "CommonMessage{" + "database='" + database + '\'' + ", table='" + table + '\'' + ", pkNames=" + pkNames
               + ", isDdl=" + isDdl + ", type='" + type + '\'' + ", es=" + es + ", ts=" + ts + ", sql='" + sql + '\''
               + ", data=" + data + ", old=" + old + '}';
    }
}
