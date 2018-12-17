package com.alibaba.otter.canal.protocol;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

/**
 * @author machengyuan 2018-9-13 下午10:31:14
 * @version 1.0.0
 */
public class FlatMessage implements Serializable {

    private static final long         serialVersionUID = -3386650678735860050L;
    private long                      id;
    private String                    database;
    private String                    table;
    private List<String>              pkNames;
    private Boolean                   isDdl;
    private String                    type;
    // binlog executeTime
    private Long                      es;
    // dml build timeStamp
    private Long                      ts;
    private String                    sql;
    private Map<String, Integer>      sqlType;
    private Map<String, String>       mysqlType;
    private List<Map<String, String>> data;
    private List<Map<String, String>> old;

    public FlatMessage() {
    }

    public FlatMessage(long id){
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
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

    public List<String> getPkNames() {
        return pkNames;
    }

    public void addPkName(String pkName) {
        if (this.pkNames == null) {
            this.pkNames = Lists.newArrayList();
        }
        this.pkNames.add(pkName);
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

    public Map<String, Integer> getSqlType() {
        return sqlType;
    }

    public void setSqlType(Map<String, Integer> sqlType) {
        this.sqlType = sqlType;
    }

    public Map<String, String> getMysqlType() {
        return mysqlType;
    }

    public void setMysqlType(Map<String, String> mysqlType) {
        this.mysqlType = mysqlType;
    }

    public List<Map<String, String>> getData() {
        return data;
    }

    public void setData(List<Map<String, String>> data) {
        this.data = data;
    }

    public List<Map<String, String>> getOld() {
        return old;
    }

    public void setOld(List<Map<String, String>> old) {
        this.old = old;
    }

    public Long getEs() {
        return es;
    }

    public void setEs(Long es) {
        this.es = es;
    }

    @Override
    public String toString() {
        return "FlatMessage [id=" + id + ", database=" + database + ", table=" + table + ", isDdl=" + isDdl + ", type="
               + type + ", es=" + es + ", ts=" + ts + ", sql=" + sql + ", sqlType=" + sqlType + ", mysqlType="
               + mysqlType + ", data=" + data + ", old=" + old + "]";
    }
}
