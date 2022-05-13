package com.alibaba.otter.canal.client.adapter.tablestore.config;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.client.adapter.support.AdapterConfig;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.Util;
import com.alibaba.otter.canal.client.adapter.tablestore.enums.TablestoreFieldType;
import com.alibaba.otter.canal.client.adapter.tablestore.support.SyncUtil;
import org.apache.commons.lang.StringUtils;

import java.sql.ResultSetMetaData;
import java.util.*;

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

    private DbMapping dbMapping;          // db映射配置

    private Boolean updateChangeColumns = false;

    private Integer threads = 8;

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

    public Boolean getUpdateChangeColumns() {
        return updateChangeColumns;
    }

    public void setUpdateChangeColumns(Boolean updateChangeColumns) {
        this.updateChangeColumns = updateChangeColumns;
    }

    public Integer getThreads() {
        return threads;
    }

    public void setThreads(Integer threads) {
        this.threads = threads;
    }

    public void validate() {
        if (dbMapping.database == null || dbMapping.database.isEmpty()) {
            throw new NullPointerException("dbMapping.database");
        }
        if (dbMapping.table == null || dbMapping.table.isEmpty()) {
            throw new NullPointerException("dbMapping.table");
        }
        if (dbMapping.targetTable == null || dbMapping.targetTable.isEmpty()) {
            throw new NullPointerException("dbMapping.targetTable");
        }
    }




    public static class ColumnItem {
        private String targetColumn;
        private String  column;
        private TablestoreFieldType type;

        public String getColumn() {
            return column;
        }

        public void setColumn(String column) {
            this.column = column;
        }

        public TablestoreFieldType getType() {
            return type;
        }

        public void setType(TablestoreFieldType type) {
            this.type = type;
        }

        public String getTargetColumn() {
            return targetColumn;
        }

        public void setTargetColumn(String targetColumn) {
            this.targetColumn = targetColumn;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ColumnItem that = (ColumnItem) o;
            return Objects.equals(column, that.column);
        }

        @Override
        public int hashCode() {
            return Objects.hash(column);
        }
    }



    public static class DbMapping implements AdapterMapping {

        private String              database;                                // 数据库名或schema名
        private String              table;                                   // 表名
        private LinkedHashMap<String, String> targetPk        = new LinkedHashMap<>(); // 目标表主键字段
//        private boolean             mapAll          = false;                 // 映射所有字段

        private String              targetTable;                             // 目标表名
        private Map<String, String> targetColumns;                           // 目标表字段映射
        private Map<String, String> targetColumnsParsed;
        private String              etlCondition;                            // etl条件sql

        private int                 readBatch       = 5000;
        private int                 commitBatch     = 5000;                  // etl等批量提交大小

        private Map<String, ColumnItem> columnItems        = new LinkedHashMap<>(); // 转换后的字段映射列表

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


        public LinkedHashMap<String, String> getTargetPk() {
            return targetPk;
        }

        public void setTargetPk(LinkedHashMap<String, String> targetPk) {
            this.targetPk = targetPk;
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

        public Map<String, String> getTargetColumnsParsed() {
            return targetColumnsParsed;
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

        public Map<String, ColumnItem> getColumnItems() {
            return columnItems;
        }

        public void setColumnItems(Map<String, ColumnItem> columnItems) {
            this.columnItems = columnItems;
        }

        public void init(MappingConfig config) {
            String splitBy = "$";
            if (targetColumns != null) {
                boolean needTypeInference = false;
                for (Map.Entry<String, String> columnField : targetColumns.entrySet()) {
                    String field = columnField.getValue();
                    String type = null;
                    if (field != null) {
                        // 解析类型
                        int i = field.indexOf(splitBy);
                        if (i > -1) {
                            type = field.substring(i + 1);
                            field = field.substring(0, i);
                        }
                    }
                    ColumnItem columnItem = new ColumnItem();
                    columnItem.setColumn(columnField.getKey());
                    columnItem.setTargetColumn(StringUtils.isBlank(field) ? columnField.getKey() : field);

                    TablestoreFieldType fieldType = SyncUtil.getTablestoreType(type);
                    if (fieldType == null) {
                        needTypeInference = true;
                    }
                    columnItem.setType(fieldType);
                    columnItems.put(columnField.getKey(), columnItem);
                }
                if (needTypeInference) {
                    // 认为有field没有配置映射类型，需要进行类型推断
                    DruidDataSource sourceDS = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());

                    Util.sqlRS(sourceDS, "SELECT * FROM " + SyncUtil.getDbTableName(database, table) + " LIMIT 1 ", rs -> {
                        try {
                            ResultSetMetaData rsd = rs.getMetaData();
                            int columnCount = rsd.getColumnCount();
                            List<String> columns = new ArrayList<>();
                            for (int i = 1; i <= columnCount; i++) {
                                String columnName = rsd.getColumnName(i);
                                if (columnItems.containsKey(columnName) && columnItems.get(columnName).getType() == null) {
                                    int columnType = rsd.getColumnType(i);
                                    columnItems.get(columnName).setType(SyncUtil.getDefaultTablestoreType(columnType));
                                }
                            }
                            return true;
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
                }

            } else {
                this.targetColumns = new LinkedHashMap<>();
            }
            targetColumnsParsed = new HashMap<>();

            targetColumns.forEach((key, value) -> {
                if (StringUtils.isEmpty(value)) {
                    targetColumnsParsed.put(key, key);
                } else if (value.contains(splitBy) && columnItems.containsKey(key)) {
                    targetColumnsParsed.put(key, columnItems.get(key).targetColumn);
                } else {
                    targetColumnsParsed.put(key, value);
                }
            });
        }

    }
}
