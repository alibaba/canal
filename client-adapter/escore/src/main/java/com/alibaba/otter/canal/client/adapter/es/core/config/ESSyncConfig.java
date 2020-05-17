package com.alibaba.otter.canal.client.adapter.es.core.config;

import com.alibaba.otter.canal.client.adapter.support.AdapterConfig;

import java.util.*;

/**
 * ES 映射配置
 *
 * @author rewerma 2018-11-01
 * @version 1.0.0
 */
public class ESSyncConfig implements AdapterConfig {

    private String    dataSourceKey;    // 数据源key

    private String    outerAdapterKey;  // adapter key

    private String    groupId;          // group id

    private String    destination;      // canal destination

    private ESMapping esMapping;

    private String    esVersion = "es6";

    public void validate() {
        if (esMapping._index == null) {
            throw new NullPointerException("esMapping._index");
        }
        if ("es6".equals(esVersion) && esMapping._type == null) {
            throw new NullPointerException("esMapping._type");
        }
        if (esMapping._id == null && esMapping.getPk() == null) {
            throw new NullPointerException("esMapping._id or esMapping.pk");
        }
        if (esMapping.sql == null) {
            throw new NullPointerException("esMapping.sql");
        }
    }

    public String getDataSourceKey() {
        return dataSourceKey;
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

    public ESMapping getEsMapping() {
        return esMapping;
    }

    public void setEsMapping(ESMapping esMapping) {
        this.esMapping = esMapping;
    }

    public ESMapping getMapping() {
        return esMapping;
    }

    public String getEsVersion() {
        return esVersion;
    }

    public void setEsVersion(String esVersion) {
        this.esVersion = esVersion;
    }

    public static class ESMapping implements AdapterMapping {

        private String                       _index;
        private String                       _type;
        private String                       _id;
        private boolean                      upsert          = false;
        private String                       pk;
        private Map<String, RelationMapping> relations       = new LinkedHashMap<>();
        private String                       sql;
        private ObjFields                    objFields       = new ObjFields();      //对象字段配置
        private List<String>                 skips           = new ArrayList<>();
        private int                          commitBatch     = 1000;
        private int                          commitBatchSize = 1048576;              // 批次提交大小（单位为字节）
        private String                       etlCondition;
        private boolean                      syncByTimestamp = false;                // 是否按时间戳定时同步
        private Long                         syncInterval;                           // 同步时间间隔

        private SchemaItem                   schemaItem;                             // sql解析结果模型

        public String get_index() {
            return _index;
        }

        public void set_index(String _index) {
            this._index = _index;
        }

        public String get_type() {
            return _type;
        }

        public void set_type(String _type) {
            this._type = _type;
        }

        public String get_id() {
            return _id;
        }

        public void set_id(String _id) {
            this._id = _id;
        }

        public boolean isUpsert() {
            return upsert;
        }

        public void setUpsert(boolean upsert) {
            this.upsert = upsert;
        }

        public String getPk() {
            return pk;
        }

        public void setPk(String pk) {
            this.pk = pk;
        }

        public ObjFields getObjFields() {
            return objFields;
        }

        public void setObjFields(ObjFields objFields) {
            this.objFields = objFields;
        }

        public List<String> getSkips() {
            return skips;
        }

        public void setSkips(List<String> skips) {
            this.skips = skips;
        }

        public Map<String, RelationMapping> getRelations() {
            return relations;
        }

        public void setRelations(Map<String, RelationMapping> relations) {
            this.relations = relations;
        }

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }

        public int getCommitBatch() {
            return commitBatch;
        }

        public void setCommitBatch(int commitBatch) {
            this.commitBatch = commitBatch;
        }

        public String getEtlCondition() {
            return etlCondition;
        }

        public void setEtlCondition(String etlCondition) {
            this.etlCondition = etlCondition;
        }

        public Long getSyncInterval() {
            return syncInterval;
        }

        public void setSyncInterval(Long syncInterval) {
            this.syncInterval = syncInterval;
        }

        public boolean isSyncByTimestamp() {
            return syncByTimestamp;
        }

        public void setSyncByTimestamp(boolean syncByTimestamp) {
            this.syncByTimestamp = syncByTimestamp;
        }

        public SchemaItem getSchemaItem() {
            return schemaItem;
        }

        public void setSchemaItem(SchemaItem schemaItem) {
            this.schemaItem = schemaItem;
        }

        public int getCommitBatchSize() {
            return commitBatchSize;
        }

        public void setCommitBatchSize(int commitBatchSize) {
            this.commitBatchSize = commitBatchSize;
        }
    }

    public static class RelationMapping {

        private String name;
        private String parent;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getParent() {
            return parent;
        }

        public void setParent(String parent) {
            this.parent = parent;
        }
    }


    /**
     * 对象字段配置
     * @author zze0 2020-05-06
     */
    public static class ObjFields {

        /**
         * 是否并行查询
         */
        private boolean                 parallel    = false;

        /**
         * 并行线程数
         */
        private int                     threadSize  = 1;

        /**
         * 对象字段
         */
        private Map<String, ObjField>   fields      = new LinkedHashMap<>();

        public boolean isParallel() {
            return parallel;
        }

        public void setParallel(boolean parallel) {
            this.parallel = parallel;
        }

        public int getThreadSize() {
            return threadSize;
        }

        public void setThreadSize(int threadSize) {
            this.threadSize = threadSize;
        }

        public Map<String, ObjField> getFields() {
            return fields;
        }

        public void setFields(Map<String, ObjField> fields) {
            this.fields = fields;
        }
    }

    /**
     * 对象字段
     * @author zze0 2020-05-06
     */
    public static class ObjField {

        /**
         * 类型
         */
        private ObjFieldType    type;

        /**
         * 子表sql（使用“${?}”来引用主表sql 字段值），如：“select _id,_name from role where _type='${_role_type}'”
         * @see ESMapping#sql
         */
        private String          sql;

        /**
         * sql解析结果
         */
        private SchemaItem      schemaItem;

        /**
         * 分隔符（当type为joining时有效）
         * @see ObjFieldType#joining
         */
        private String          separator;

        /**
         * 是否反向更新（true表示当子表有数据变动时，也会同步更新到es中）
         */
        private boolean         reverseUpdate = true;

        public ObjFieldType getType() {
            return type;
        }

        public void setType(ObjFieldType type) {
            this.type = type;
        }

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }

        public SchemaItem getSchemaItem() {
            return schemaItem;
        }

        public void setSchemaItem(SchemaItem schemaItem) {
            this.schemaItem = schemaItem;
        }

        public String getSeparator() {
            return Objects.toString(separator, "");
        }

        public void setSeparator(String separator) {
            this.separator = separator;
        }

        public boolean isReverseUpdate() {
            return reverseUpdate;
        }

        public void setReverseUpdate(boolean reverseUpdate) {
            this.reverseUpdate = reverseUpdate;
        }
    }

    /**
     * 对象类型
     */
    public enum ObjFieldType {

        /**
         * 数组，如: ["1","2","3","4"]
         */
        array,

        /**
         * JSON对象，如: {"key":"value"}
         */
        object,

        /**
         * JSON对象数组，如: [{"key":"value"}]
         */
        objectArray,

        /**
         * 字符串拼接（搭配ObjField#separator分隔符使用），如: “1,2,3,4”
         * @see ObjField#separator
         */
        joining,

        /**
         * JSON对象扁平化，如: {"key":"value"} <p/>
         * 将该子表sql的数据全部扁平化放入主表数据中，而不作为某个对象字段存在。
         */
        objectFlat,
    }
}
