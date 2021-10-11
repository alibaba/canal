/**
 * Created by Wu Jian Ping on - 2021/09/15.
 */

package com.alibaba.otter.canal.client.adapter.http.config;

import java.util.*;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.adapter.support.AdapterConfig;

public class MappingConfig implements AdapterConfig {

    private String dataSourceKey; // 数据源key

    private String destination; // canal实例或MQ的topic

    private String groupId; // groupId

    private String outerAdapterKey; // 对应适配器的key

    private HttpMapping httpMapping; // hbase映射配置

    @Override
    public String getDataSourceKey() {
        return dataSourceKey;
    }

    public void setDataSourceKey(String dataSourceKey) {
        this.dataSourceKey = dataSourceKey;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
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

    public void validate() {
    }

    public HttpMapping getHttpMapping() {
        return httpMapping;
    }

    public void setHttpMapping(HttpMapping httpMapping) {
        this.httpMapping = httpMapping;
    }

    @Override
    public HttpMapping getMapping() {
        return this.httpMapping;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

    public static class HttpMapping implements AdapterMapping {
        private String etlCondition; // etl条件sql
        private List<MonitorTable> monitorTables = new ArrayList<>();
        private EtlSetting etlSetting;

        public String getEtlCondition() {
            return etlCondition;
        }

        public void setEtlCondition(String etlCondition) {
            this.etlCondition = etlCondition;
        }

        public List<MonitorTable> getMonitorTables() {
            return this.monitorTables;
        }

        public void setMonitorTables(List<MonitorTable> monitorTables) {
            this.monitorTables = monitorTables;
        }

        public EtlSetting getEtlSetting() {
            return this.etlSetting;
        }

        public void setEtlSetting(EtlSetting etlSetting) {
            this.etlSetting = etlSetting;
        }
    }

    public static class EtlSetting {
        private String database;
        private String table;
        private String condition;
        private int batchSize = 1000;
        private int threads = Runtime.getRuntime().availableProcessors();

        public String getDatabase() {
            return this.database;
        }

        public void setDatabase(String database) {
            this.database = database;
        }

        public String getTable() {
            return this.table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public String getCondition() {
            return this.condition;
        }

        public void setCondition(String condition) {
            this.condition = condition;
        }

        public int getBatchSize() {
            return this.batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public int getThreads() {
            return this.threads;
        }

        public void setThreads(int threads) {
            this.threads = threads;
        }
    }

    public static class MonitorTable {
        private String tableName;
        private List<String> actions = new ArrayList<>();

        public String getTableName() {
            return this.tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public List<String> getActions() {
            return this.actions;
        }

        public void setActions(List<String> actions) {
            this.actions = actions;
        }
    }
}
