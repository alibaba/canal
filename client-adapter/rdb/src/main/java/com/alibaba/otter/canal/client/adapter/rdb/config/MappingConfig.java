package com.alibaba.otter.canal.client.adapter.rdb.config;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.alibaba.otter.canal.client.adapter.support.AdapterConfig;

/**
 * RDB表映射配置
 *
 * @author rewerma 2018-11-07 下午02:41:34
 * @version 1.0.0
 */
public class MappingConfig implements AdapterConfig {

    private String    dataSourceKey;      // 数据源key

    private String    destination;        // canal实例或MQ的topic

    private String    groupId;            // groupId

    private String    outerAdapterKey;    // 对应适配器的key

    private boolean   concurrent = false; // 是否并行同步

    private DbMapping dbMapping;          // db映射配置

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

    public boolean getConcurrent() {
        return concurrent;
    }

    public void setConcurrent(boolean concurrent) {
        this.concurrent = concurrent;
    }

    public DbMapping getDbMapping() {
        return dbMapping;
    }

    public void setDbMapping(DbMapping dbMapping) {
        this.dbMapping = dbMapping;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public AdapterMapping getMapping() {
        return dbMapping;
    }

    public void validate() {
        if (dbMapping.database == null || dbMapping.database.isEmpty()) {
            throw new NullPointerException("dbMapping.database");
        }
        if (!dbMapping.getMirrorDb() && (dbMapping.table == null || dbMapping.table.isEmpty())) {
            throw new NullPointerException("dbMapping.table");
        }
        if (!dbMapping.getMirrorDb() && (dbMapping.targetTable == null || dbMapping.targetTable.isEmpty())) {
            throw new NullPointerException("dbMapping.targetTable");
        }
    }

    public static class DbMapping implements AdapterMapping {

        private boolean             mirrorDb        = false;                 // 是否镜像库
        private String              database;                                // 数据库名或schema名
        private String              table;                                   // 表名
        private Map<String, String> targetPk        = new LinkedHashMap<>(); // 目标表主键字段
        private boolean             mapAll          = false;                 // 映射所有字段
        private String              targetDb;                                // 目标库名
        private String              targetTable;                             // 目标表名
        private Map<String, String> targetColumns;                           // 目标表字段映射

        private boolean             caseInsensitive = false;                 // 目标表不区分大小写，默认是否

        private String              etlCondition;                            // etl条件sql

        private int                 readBatch       = 5000;
        private int                 commitBatch     = 5000;                  // etl等批量提交大小

        private Map<String, String> allMapColumns;

        public boolean getMirrorDb() {
            return mirrorDb;
        }

        public void setMirrorDb(boolean mirrorDb) {
            this.mirrorDb = mirrorDb;
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

        public Map<String, String> getTargetPk() {
            return targetPk;
        }

        public void setTargetPk(Map<String, String> targetPk) {
            this.targetPk = targetPk;
        }

        public Boolean getMapAll() {
            return mapAll;
        }

        public void setMapAll(Boolean mapAll) {
            this.mapAll = mapAll;
        }

        public String getTargetDb() {
            return targetDb;
        }

        public void setTargetDb(String targetDb) {
            this.targetDb = targetDb;
        }

        public String getTargetTable() {
            return targetTable;
        }

        public void setTargetTable(String targetTable) {
            this.targetTable = targetTable;
        }

        public Map<String, String> getTargetColumns() {
            if (targetColumns != null) {
                targetColumns.forEach((key, value) -> {
                    if (StringUtils.isEmpty(value)) {
                        targetColumns.put(key, key);
                    }
                });
            }
            return targetColumns;
        }

        public void setTargetColumns(Map<String, String> targetColumns) {
            this.targetColumns = targetColumns;
        }

        public boolean isCaseInsensitive() {
            return caseInsensitive;
        }

        public void setCaseInsensitive(boolean caseInsensitive) {
            this.caseInsensitive = caseInsensitive;
        }

        public String getEtlCondition() {
            return etlCondition;
        }

        public void setEtlCondition(String etlCondition) {
            this.etlCondition = etlCondition;
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

        public Map<String, String> getAllMapColumns() {
            return allMapColumns;
        }

        public void setAllMapColumns(Map<String, String> allMapColumns) {
            this.allMapColumns = allMapColumns;
        }
    }
}
