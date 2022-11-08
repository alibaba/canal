package com.alibaba.otter.canal.parse.inbound.mysql.ddl;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;

/**
 * @author agapple 2017年8月1日 下午7:30:42
 * @since 3.2.5
 */
public class DdlResult {

    private String    schemaName;
    private String    tableName;
    private String    oriSchemaName;    // rename ddl中的源表
    private String    oriTableName;     // rename ddl中的目标表
    private EventType type;
    private DdlResult renameTableResult; // 多个rename table的存储

    /*
     * RENAME TABLE tbl_name TO new_tbl_name [, tbl_name2 TO new_tbl_name2] ...
     */

    public DdlResult(){
    }

    public DdlResult(String schemaName){
        this.schemaName = schemaName;
    }

    public DdlResult(String schemaName, String tableName){
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public DdlResult(String schemaName, String tableName, String oriSchemaName, String oriTableName){
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.oriSchemaName = oriSchemaName;
        this.oriTableName = oriTableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public EventType getType() {
        return type;
    }

    public void setType(EventType type) {
        this.type = type;
    }

    public String getOriSchemaName() {
        return oriSchemaName;
    }

    public void setOriSchemaName(String oriSchemaName) {
        this.oriSchemaName = oriSchemaName;
    }

    public String getOriTableName() {
        return oriTableName;
    }

    public void setOriTableName(String oriTableName) {
        this.oriTableName = oriTableName;
    }

    public DdlResult getRenameTableResult() {
        return renameTableResult;
    }

    public void setRenameTableResult(DdlResult renameTableResult) {
        this.renameTableResult = renameTableResult;
    }

    @Override
    public DdlResult clone() {
        DdlResult result = new DdlResult();
        result.setOriSchemaName(oriSchemaName);
        result.setOriTableName(oriTableName);
        result.setSchemaName(schemaName);
        result.setTableName(tableName);
        // result.setType(type);
        return result;
    }

    @Override
    public String toString() {
        DdlResult ddlResult = this;
        StringBuffer sb = new StringBuffer();
        do {
            sb.append(String.format("DdlResult [schemaName=%s , tableName=%s , oriSchemaName=%s , oriTableName=%s , type=%s ];",
                ddlResult.schemaName,
                ddlResult.tableName,
                ddlResult.oriSchemaName,
                ddlResult.oriTableName,
                ddlResult.type));
            ddlResult = ddlResult.renameTableResult;
        } while (ddlResult != null);
        return sb.toString();
    }
}
