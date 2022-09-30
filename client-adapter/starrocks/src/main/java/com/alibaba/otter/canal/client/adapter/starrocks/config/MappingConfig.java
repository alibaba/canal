package com.alibaba.otter.canal.client.adapter.starrocks.config;

import com.alibaba.otter.canal.client.adapter.support.AdapterConfig;

/**
 * StarRocks表映射配置
 */
public class MappingConfig implements AdapterConfig {

    private String      dataSourceKey;   // 数据源key

    private String      outerAdapterKey; // adapter key

    private String      groupId;         // groupId

    private String      destination;     // canal实例或MQ的topic

    private SrMapping srMapping;

    @Override
    public String getDataSourceKey() {
        return dataSourceKey;
    }

    @Override
    public AdapterMapping getMapping() {
        return  srMapping;
    }

    public void setDataSourceKey(String dataSourceKey) {
        this.dataSourceKey = dataSourceKey;
    }

    public String getOuterAdapterKey() {
        return outerAdapterKey;
    }

    public void setOuterAdapterKey(String outerAdapterKey) {
        this.outerAdapterKey = outerAdapterKey;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public SrMapping getSrMapping() {
        return srMapping;
    }

    public void setSrMapping(SrMapping srMapping) {
        this.srMapping = srMapping;
    }

    public void validate() {
        if (srMapping.database == null || srMapping.database.isEmpty()) {
            throw new NullPointerException("srMapping.database");
        }

        if (srMapping.table == null || srMapping.table.isEmpty()) {
            throw new NullPointerException("srMapping.table");
        }

        if (srMapping.srTable == null || srMapping.srTable.isEmpty()) {
            throw new NullPointerException("srMapping.srTable");
        }
    }

    public static class SrMapping implements AdapterMapping {

        private String database;

        private String table;

        private String eventType;

        private String srTable;

        private String  etlCondition;

        private int commitBatch = 5000;

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

        public String getEventType() {
            return eventType;
        }

        public void setEventType(String eventType) {
            this.eventType = eventType;
        }

        public String getSrTable() {
            return srTable;
        }

        public void setSrTable(String srTable) {
            this.srTable = srTable;
        }

        public String getEtlCondition() {
            return etlCondition;
        }

        public void setEtlCondition(String etlCondition) {
            this.etlCondition = etlCondition;
        }

        public int getCommitBatch() {
            return commitBatch;
        }

        public void setCommitBatch(int commitBatch) {
            this.commitBatch = commitBatch;
        }

    }
}
