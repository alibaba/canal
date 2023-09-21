package com.alibaba.otter.canal.client.adapter.support;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class YamlUtilsTest {



    @Test
    public void testLoadConfigToYml() {
        String configStr="dataSourceKey: defaultDS\n"
            + "destination: example\n"
            + "groupId: g1\n"
            + "outerAdapterKey: mysql1\n"
            + "concurrent: true\n"
            + "dbMapping:\n"
            + "  _id: _id\n"
            + "  database: mytest\n"
            + "  table: user\n"
            + "  targetTable: mytest2.user\n"
            + "  targetPk:\n"
            + "    id: id\n"
            + "#  mapAll: true\n"
            + "  targetColumns:\n"
            + "    id:\n"
            + "    name:\n"
            + "    role_id:\n"
            + "    c_time:\n"
            + "    test1:\n"
            + "  etlCondition: \"where c_time>={}\"\n"
            + "  commitBatch: 3000 # 批量提交的大小";

        MappingConfig config = YamlUtils.ymlToObj(null, configStr, MappingConfig.class, null, new Properties());

        Assert.assertNotNull(config);
        Assert.assertEquals(config.getDbMapping().getId(), "_id");
        Assert.assertEquals(config.getDestination(), "example");
        Assert.assertEquals(config.getOuterAdapterKey(), "mysql1");
        Assert.assertEquals(config.getDbMapping().getDatabase(), "mytest");
        Assert.assertEquals(config.getDbMapping().getTargetColumns().size(), 5);
    }

    private static class MappingConfig {
        private String    dataSourceKey;

        private String    destination;

        private String    groupId;

        private String    outerAdapterKey;

        private boolean   concurrent = false;

        private DbMapping dbMapping;

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

        public boolean isConcurrent() {
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
    }

    private static class DbMapping {

        @Value("${_id}")
        private String id ;
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

        public boolean isMirrorDb() {
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

        public boolean isMapAll() {
            return mapAll;
        }

        public void setMapAll(boolean mapAll) {
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

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }
    }
}
