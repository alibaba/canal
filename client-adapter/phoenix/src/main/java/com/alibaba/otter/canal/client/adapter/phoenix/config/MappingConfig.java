package com.alibaba.otter.canal.client.adapter.phoenix.config;

import org.apache.commons.lang.StringUtils;

import java.util.*;

/**
 * Phoenix表映射配置
 */
@SuppressWarnings("unused")
public class MappingConfig {

    private String dataSourceKey;           // 数据源key
    private String destination;             // canal实例或MQ的topic
    private String groupId;                 // groupId
    private String outerAdapterKey;         // 对应适配器的key
    private boolean concurrent = false;     // 是否并行同步
    private DbMapping dbMapping;            // db映射配置
    private boolean debug = false;          // 调试

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

    public boolean isDebug() {
        return debug;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public void validate() {
        if (dbMapping.database == null || dbMapping.database.isEmpty()) {
            throw new NullPointerException("dbMapping.database");
        }
        if ((dbMapping.table == null || dbMapping.table.isEmpty())) {
            throw new NullPointerException("dbMapping.table");
        }
        if ((dbMapping.targetTable == null || dbMapping.targetTable.isEmpty())) {
            throw new NullPointerException("dbMapping.targetTable");
        }
    }

    public static class DbMapping {

        private String database;                            // 数据库名或schema名
        private String table;                               // 表名
        private Map<String, String> targetPk = new LinkedHashMap<>(); // 目标表主键字段
        private boolean mapAll = true;                      // 映射所有字段

        private boolean alter = true;                       // 是否允许修改表
        private boolean drop = false;                       // 是否允许删除字段
        private boolean limit = false;                      // 是否限制字段长度
        private boolean skipMissing = false;                // 是否跳过丢失的字段
        private boolean escapeUpper = true;                 // 字段默认大写加双引号

        private String targetDb;                            // 目标库名
        private String targetTable;                         // 目标表名
        private Map<String, String> targetColumns;          // 目标表字段映射

        private List<String> excludeColumns;                // 不映射的字段

        private String etlCondition;                        // etl条件sql
        private int readBatch = 5000;
        private int commitBatch = 5000;                     // etl等批量提交大小
        private Map<String, String> allMapColumns;

        public String escape(String name) {
            if (escapeUpper) {
                return "\"" + name.toUpperCase() + "\"";
            } else {
                return name;
            }
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

        public boolean isAlter() {
            return alter;
        }

        public void setAlter(boolean alter) {
            this.alter = alter;
        }

        public boolean isLimit() {
            return limit;
        }

        public void setLimit(boolean limit) {
            this.limit = limit;
        }

        public boolean isDrop() {
            return drop;
        }

        public void setDrop(boolean drop) {
            this.drop = drop;
        }

        public boolean isSkipMissing() {
            return skipMissing;
        }

        public void setSkipMissing(boolean skipMissing) {
            this.skipMissing = skipMissing;
        }

        public boolean isEscapeUpper() {
            return escapeUpper;
        }

        public void setEscapeUpper(boolean escapeUpper) {
            this.escapeUpper = escapeUpper;
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
            } else {
                targetColumns = new HashMap<>();
            }
            return targetColumns;
        }

        public void setTargetColumns(Map<String, String> targetColumns) {
            this.targetColumns = targetColumns;
        }

        public void addTargetColumn(String key, String value) {
            if (targetColumns == null) {
                targetColumns = new HashMap<>();
            }
            targetColumns.put(key, value);
            if (allMapColumns != null) {
                allMapColumns.put(key, value);
            }
        }

        public void removeTargetColumn(String key) {
            if (targetColumns != null) {
                targetColumns.remove(key);
            }
            if (allMapColumns != null) {
                allMapColumns.remove(key);
            }
        }

        public List<String> getExcludeColumns() {
            if (excludeColumns == null) {
                excludeColumns = new ArrayList<>();
            }
            return excludeColumns;
        }

        public void setExcludeColumns(List<String> excludeColumns) {
            this.excludeColumns = excludeColumns;
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
