package com.alibaba.otter.canal.client.adapter.redis.config;

import java.util.Map;

import com.alibaba.otter.canal.client.adapter.support.AdapterConfig;
import org.apache.commons.lang.StringUtils;

/**
 * RDB表映射配置
 *
 * @author rewerma 2018-11-07 下午02:41:34
 * @version 1.0.0
 */
public class MappingConfig implements AdapterConfig {

    private String dataSourceKey;      // 数据源key

    private String destination;        // canal实例或MQ的topic

    private String groupId;            // groupId

    private String outerAdapterKey;    // 对应适配器的key

    private RedisMapping redisMapping;          // redis 映射配置

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

    public RedisMapping getRedisMapping() {
        return redisMapping;
    }

    public void setRedisMapping(RedisMapping redisMapping) {
        this.redisMapping = redisMapping;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public AdapterMapping getMapping() {
        return redisMapping;
    }

    public void validate() {
        if (redisMapping.database == null || redisMapping.database.isEmpty()) {
            throw new NullPointerException("redisMapping.database");
        }

        if (redisMapping.table == null || redisMapping.table.isEmpty()) {
            throw new NullPointerException("redisMapping.table");
        }

        if (redisMapping.key == null || redisMapping.key.isEmpty()) {
            throw new NullPointerException("redisMapping.key");
        } else {
            if (redisMapping.key.contains("{}") && (redisMapping.pk == null || redisMapping.pk.isEmpty())) {
                throw new NullPointerException("redisMapping.pk");
            }
        }
    }

    public static class RedisMapping implements AdapterMapping {
        private String database;                            // 数据库名或schema名
        private String table;                               // 表名
        private String pk       = null;                     // 主键名
        private String key;                                 // key
        private String dataType = "string";                 // 数据类型
        private int expire;                                 // 过期时间
        private boolean mapAll = false;                     // 映射所有字段
        private Map<String, String> targetColumns;          // 目标表字段映射

        private String etlCondition;                        // etl条件sql

        private Map<String, String> allMapColumns;

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

        public Boolean getMapAll() {
            return mapAll;
        }

        public void setMapAll(Boolean mapAll) {
            this.mapAll = mapAll;
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

        public String getEtlCondition() {
            return etlCondition;
        }

        public void setEtlCondition(String etlCondition) {
            this.etlCondition = etlCondition;
        }

        public String getPk() {
            return pk;
        }

        public void setPk(String pk) {
            this.pk = pk;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public Map<String, String> getAllMapColumns() {
            return allMapColumns;
        }

        public void setAllMapColumns(Map<String, String> allMapColumns) {
            this.allMapColumns = allMapColumns;
        }

        public int getExpire() {
            return expire;
        }

        public void setExpire(int expire) {
            this.expire = expire;
        }

        public String getDataType() {
            return dataType;
        }

        public void setDataType(String dataType) {
            this.dataType = dataType;
        }
    }
}
