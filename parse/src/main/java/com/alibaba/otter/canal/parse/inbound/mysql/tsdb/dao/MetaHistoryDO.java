package com.alibaba.otter.canal.parse.inbound.mysql.tsdb.dao;

import java.util.Date;

/**
 * @author agapple 2017年7月27日 下午11:09:41
 * @since 1.0.25
 */
public class MetaHistoryDO {

    private Long   id;
    private Date   gmtCreate;
    private Date   gmtModified;
    private String destination;
    private String binlogFile;
    private Long   binlogOffest;
    private String binlogMasterId;
    private Long   binlogTimestamp;
    private String useSchema;
    private String sqlSchema;
    private String sqlTable;
    private String sqlText;
    private String sqlType;
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

    public String getExtra() {
        return extra;
    }

    public void setExtra(String extra) {
        this.extra = extra;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public String getSqlSchema() {
        return sqlSchema;
    }

    public void setSqlSchema(String sqlSchema) {
        this.sqlSchema = sqlSchema;
    }

    public String getSqlTable() {
        return sqlTable;
    }

    public void setSqlTable(String sqlTable) {
        this.sqlTable = sqlTable;
    }

    public String getSqlText() {
        return sqlText;
    }

    public void setSqlText(String sqlText) {
        this.sqlText = sqlText;
    }

    public String getSqlType() {
        return sqlType;
    }

    public void setSqlType(String sqlType) {
        this.sqlType = sqlType;
    }

    @Override
    public String toString() {
        return "MetaHistoryDO [id=" + id + ", gmtCreate=" + gmtCreate + ", gmtModified=" + gmtModified
               + ", destination=" + destination + ", binlogFile=" + binlogFile + ", binlogOffest=" + binlogOffest
               + ", binlogMasterId=" + binlogMasterId + ", binlogTimestamp=" + binlogTimestamp + ", useSchema="
               + useSchema + ", sqlSchema=" + sqlSchema + ", sqlTable=" + sqlTable + ", sqlText=" + sqlText
               + ", sqlType=" + sqlType + ", extra=" + extra + "]";
    }

}
