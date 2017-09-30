package com.alibaba.otter.canal.parse.inbound.mysql.tsdb.model;

import java.util.Date;

/**
 * @author agapple 2017年7月27日 下午11:09:41
 * @since 3.2.5
 */
public class MetaHistoryDO {

    /**
     * 主键
     */
    private Long   id;

    /**
     * 创建时间
     */
    private Date   gmtCreate;

    /**
     * 修改时间
     */
    private Date   gmtModified;

    private String binlogFile;
    private Long   binlogOffest;
    private String binlogMasterId;
    private Long   binlogTimestamp;
    private String useSchema;
    private String schema;
    private String table;
    private String sql;
    private String type;
    private String extra;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Date getGmtCreate() {
        return gmtCreate;
    }

    public void setGmtCreate(Date gmtCreate) {
        this.gmtCreate = gmtCreate;
    }

    public Date getGmtModified() {
        return gmtModified;
    }

    public void setGmtModified(Date gmtModified) {
        this.gmtModified = gmtModified;
    }

    public String getBinlogFile() {
        return binlogFile;
    }

    public void setBinlogFile(String binlogFile) {
        this.binlogFile = binlogFile;
    }

    public Long getBinlogOffest() {
        return binlogOffest;
    }

    public void setBinlogOffest(Long binlogOffest) {
        this.binlogOffest = binlogOffest;
    }

    public String getBinlogMasterId() {
        return binlogMasterId;
    }

    public void setBinlogMasterId(String binlogMasterId) {
        this.binlogMasterId = binlogMasterId;
    }

    public Long getBinlogTimestamp() {
        return binlogTimestamp;
    }

    public void setBinlogTimestamp(Long binlogTimestamp) {
        this.binlogTimestamp = binlogTimestamp;
    }

    public String getUseSchema() {
        return useSchema;
    }

    public void setUseSchema(String useSchema) {
        this.useSchema = useSchema;
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

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getExtra() {
        return extra;
    }

    public void setExtra(String extra) {
        this.extra = extra;
    }

}
