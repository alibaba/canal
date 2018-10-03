package com.alibaba.otter.canal.client.adapter.support;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * DML操作转换对象
 *
 * @author machengyuan 2018-8-19 下午11:30:49
 * @version 1.0.0
 */
public class Dml implements Serializable {

    private static final long         serialVersionUID = 2611556444074013268L;

    private String                    database;
    private String                    table;
    private String                    type;
    private Long                      ts;
    private String                    sql;
    private List<Map<String, Object>> data;
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

    public void clear() {
        database = null;
        table = null;
        type = null;
        ts = null;
        data = null;
        old = null;
        sql = null;
    }

    @Override
    public String toString() {
        return "Dml{" + "database='" + database + '\'' + ", table='" + table + '\'' + ", type='" + type + '\''
               + ", ts=" + ts + ", sql='" + sql + '\'' + ", data=" + data + ", old=" + old + '}';
    }
}
