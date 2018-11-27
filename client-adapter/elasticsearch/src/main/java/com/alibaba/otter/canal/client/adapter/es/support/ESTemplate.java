package com.alibaba.otter.canal.client.adapter.es.support;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig.ESMapping;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem.ColumnItem;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem.FieldItem;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;

/**
 * ES 操作模板
 *
 * @author rewerma 2018-11-01
 * @version 1.0.0
 */
public class ESTemplate {

    private static final Logger logger         = LoggerFactory.getLogger(ESTemplate.class);

    private static final int    MAX_BATCH_SIZE = 1000;

    private TransportClient     transportClient;

    public ESTemplate(TransportClient transportClient){
        this.transportClient = transportClient;
    }

    /**
     * 插入数据
     * 
     * @param mapping
     * @param pkVal
     * @param esFieldData
     * @return
     */
    public boolean insert(ESMapping mapping, Object pkVal, Map<String, Object> esFieldData) {
        BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();
        if (mapping.get_id() != null) {
            bulkRequestBuilder
                .add(transportClient.prepareIndex(mapping.get_index(), mapping.get_type(), pkVal.toString())
                    .setSource(esFieldData));
        } else {
            SearchResponse response = transportClient.prepareSearch(mapping.get_index())
                .setTypes(mapping.get_type())
                .setQuery(QueryBuilders.termQuery(mapping.getPk(), pkVal))
                .setSize(MAX_BATCH_SIZE)
                .get();
            for (SearchHit hit : response.getHits()) {
                bulkRequestBuilder
                    .add(transportClient.prepareDelete(mapping.get_index(), mapping.get_type(), hit.getId()));
            }
            bulkRequestBuilder
                .add(transportClient.prepareIndex(mapping.get_index(), mapping.get_type()).setSource(esFieldData));
        }
        return commitBulkRequest(bulkRequestBuilder);
    }

    /**
     * 根据主键更新数据
     * 
     * @param mapping
     * @param pkVal
     * @param esFieldData
     * @return
     */
    public boolean update(ESMapping mapping, Object pkVal, Map<String, Object> esFieldData) {
        BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();
        append4Update(bulkRequestBuilder, mapping, pkVal, esFieldData);
        return commitBulkRequest(bulkRequestBuilder);
    }

    public void append4Update(BulkRequestBuilder bulkRequestBuilder, ESMapping mapping, Object pkVal,
                              Map<String, Object> esFieldData) {
        if (mapping.get_id() != null) {
            bulkRequestBuilder
                .add(transportClient.prepareUpdate(mapping.get_index(), mapping.get_type(), pkVal.toString())
                    .setDoc(esFieldData));
        } else {
            SearchResponse response = transportClient.prepareSearch(mapping.get_index())
                .setTypes(mapping.get_type())
                .setQuery(QueryBuilders.termQuery(mapping.getPk(), pkVal))
                .setSize(MAX_BATCH_SIZE)
                .get();
            for (SearchHit hit : response.getHits()) {
                bulkRequestBuilder
                    .add(transportClient.prepareUpdate(mapping.get_index(), mapping.get_type(), hit.getId())
                        .setDoc(esFieldData));
            }
        }
    }

    /**
     * update by query
     *
     * @param config
     * @param paramsTmp
     * @param esFieldData
     * @return
     */
    public boolean updateByQuery(ESSyncConfig config, Map<String, Object> paramsTmp, Map<String, Object> esFieldData) {
        if (paramsTmp.isEmpty()) {
            return false;
        }
        ESMapping mapping = config.getEsMapping();
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        paramsTmp.forEach((fieldName, value) -> queryBuilder.must(QueryBuilders.termsQuery(fieldName, value)));

        SearchResponse response = transportClient.prepareSearch(mapping.get_index())
            .setTypes(mapping.get_type())
            .setSize(0)
            .setQuery(queryBuilder)
            .get();
        long count = response.getHits().getTotalHits();
        // 如果更新量大于Max, 查询sql批量更新
        if (count > MAX_BATCH_SIZE) {
            BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();

            DataSource ds = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
            // 查询sql更新
            StringBuilder sql = new StringBuilder("SELECT * FROM (" + mapping.getSql() + ") _v WHERE ");
            paramsTmp.forEach(
                (fieldName, value) -> sql.append("_v.").append(fieldName).append("=").append(value).append(" AND "));
            int len = sql.length();
            sql.delete(len - 4, len);
            ESSyncUtil.sqlRS(ds, sql.toString(), rs -> {
                int exeCount = 1;
                try {
                    BulkRequestBuilder bulkRequestBuilderTmp = bulkRequestBuilder;
                    while (rs.next()) {
                        Object idVal = getIdValFromRS(mapping, rs);
                        append4Update(bulkRequestBuilderTmp, mapping, idVal, esFieldData);

                        if (exeCount % mapping.getCommitBatch() == 0 && bulkRequestBuilderTmp.numberOfActions() > 0) {
                            commitBulkRequest(bulkRequestBuilderTmp);
                            bulkRequestBuilderTmp = transportClient.prepareBulk();
                        }
                        exeCount++;
                    }

                    if (bulkRequestBuilder.numberOfActions() > 0) {
                        commitBulkRequest(bulkRequestBuilderTmp);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return 0;
            });
            return true;
        } else {
            return updateByQuery(mapping, queryBuilder, esFieldData, 1);
        }
    }

    private boolean updateByQuery(ESMapping mapping, QueryBuilder queryBuilder, Map<String, Object> esFieldData,
                                  int counter) {
        if (CollectionUtils.isEmpty(esFieldData)) {
            return true;
        }

        StringBuilder sb = new StringBuilder();
        esFieldData.forEach((key, value) -> {
            if (value instanceof Map) {
                HashMap mapValue = (HashMap) value;
                if (mapValue.containsKey("lon") && mapValue.containsKey("lat") && mapValue.size() == 2) {
                    sb.append("ctx._source")
                        .append("['")
                        .append(key)
                        .append("']")
                        .append(" = [")
                        .append(mapValue.get("lon"))
                        .append(", ")
                        .append(mapValue.get("lat"))
                        .append("];");
                } else {
                    sb.append("ctx._source").append("[\"").append(key).append("\"]").append(" = ");
                    sb.append(JSON.toJSONString(value));
                    sb.append(";");
                }
            } else if (value instanceof List) {
                sb.append("ctx._source").append("[\"").append(key).append("\"]").append(" = ");
                sb.append(JSON.toJSONString(value));
                sb.append(";");
            } else if (value instanceof String) {
                sb.append("ctx._source")
                    .append("['")
                    .append(key)
                    .append("']")
                    .append(" = '")
                    .append(value)
                    .append("';");
            } else {
                sb.append("ctx._source").append("['").append(key).append("']").append(" = ").append(value).append(";");
            }
        });
        String scriptLine = sb.toString();
        if (logger.isTraceEnabled()) {
            logger.trace(scriptLine);
        }

        UpdateByQueryRequestBuilder updateByQuery = UpdateByQueryAction.INSTANCE.newRequestBuilder(transportClient);
        updateByQuery.source(mapping.get_index())
            .abortOnVersionConflict(false)
            .filter(queryBuilder)
            .script(new Script(ScriptType.INLINE, "painless", scriptLine, Collections.emptyMap()));

        BulkByScrollResponse response = updateByQuery.get();
        if (logger.isTraceEnabled()) {
            logger.trace("updateByQuery response: {}", response.getStatus());
        }
        if (!CollectionUtils.isEmpty(response.getSearchFailures())) {
            logger.error("script update_for_search has search error: " + response.getBulkFailures());
            return false;
        }

        if (!CollectionUtils.isEmpty(response.getBulkFailures())) {
            logger.error("script update_for_search has update error: " + response.getBulkFailures());
            return false;
        }

        if (response.getStatus().getVersionConflicts() > 0) {
            if (counter >= 3) {
                logger.error("第 {} 次执行updateByQuery, 依旧存在分片版本冲突，不再继续重试。", counter);
                return false;
            }
            logger.warn("本次updateByQuery存在分片版本冲突，准备重新执行...");
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                // ignore
            }
            return updateByQuery(mapping, queryBuilder, esFieldData, ++counter);
        }

        return true;
    }

    /**
     * 通过主键删除数据
     *
     * @param mapping
     * @param pkVal
     * @return
     */
    public boolean delete(ESMapping mapping, Object pkVal) {
        BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();
        if (mapping.get_id() != null) {
            bulkRequestBuilder
                .add(transportClient.prepareDelete(mapping.get_index(), mapping.get_type(), pkVal.toString()));
        } else {
            SearchResponse response = transportClient.prepareSearch(mapping.get_index())
                .setTypes(mapping.get_type())
                .setQuery(QueryBuilders.termQuery(mapping.getPk(), pkVal))
                .setSize(MAX_BATCH_SIZE)
                .get();
            for (SearchHit hit : response.getHits()) {
                bulkRequestBuilder
                    .add(transportClient.prepareDelete(mapping.get_index(), mapping.get_type(), hit.getId()));
            }
        }
        return commitBulkRequest(bulkRequestBuilder);
    }

    /**
     * 批量提交
     *
     * @param bulkRequestBuilder
     * @return
     */
    private static boolean commitBulkRequest(BulkRequestBuilder bulkRequestBuilder) {
        if (bulkRequestBuilder.numberOfActions() > 0) {
            BulkResponse response = bulkRequestBuilder.execute().actionGet();
            if (response.hasFailures()) {
                for (BulkItemResponse itemResponse : response.getItems()) {
                    if (!itemResponse.isFailed()) {
                        continue;
                    }

                    if (itemResponse.getFailure().getStatus() == RestStatus.NOT_FOUND) {
                        logger.warn(itemResponse.getFailureMessage());
                    } else {
                        logger.error("ES sync commit error: {}", itemResponse.getFailureMessage());
                    }
                }
            }

            return !response.hasFailures();
        }
        return true;
    }

    public Object getValFromRS(ESMapping mapping, ResultSet resultSet, String fieldName,
                               String columnName) throws SQLException {
        String esType = getEsType(mapping, fieldName);

        Object value = resultSet.getObject(columnName);
        if (value instanceof Boolean) {
            if (!"boolean".equals(esType)) {
                value = resultSet.getByte(columnName);
            }
        }

        // 如果是对象类型
        if (mapping.getObjFields().containsKey(fieldName)) {
            return ESSyncUtil.convertToEsObj(value, mapping.getObjFields().get(fieldName));
        } else {
            return ESSyncUtil.typeConvert(value, esType);
        }
    }

    public Object getESDataFromRS(ESMapping mapping, ResultSet resultSet,
                                  Map<String, Object> esFieldData) throws SQLException {
        SchemaItem schemaItem = mapping.getSchemaItem();
        String idFieldName = mapping.get_id() == null ? mapping.getPk() : mapping.get_id();
        Object resultIdVal = null;
        for (FieldItem fieldItem : schemaItem.getSelectFields().values()) {
            Object value = getValFromRS(mapping, resultSet, fieldItem.getFieldName(), fieldItem.getFieldName());

            if (fieldItem.getFieldName().equals(idFieldName)) {
                resultIdVal = value;
            }

            if (!fieldItem.getFieldName().equals(mapping.get_id())
                && !mapping.getSkips().contains(fieldItem.getFieldName())) {
                esFieldData.put(fieldItem.getFieldName(), value);
            }
        }
        return resultIdVal;
    }

    public Object getIdValFromRS(ESMapping mapping, ResultSet resultSet) throws SQLException {
        SchemaItem schemaItem = mapping.getSchemaItem();
        String idFieldName = mapping.get_id() == null ? mapping.getPk() : mapping.get_id();
        Object resultIdVal = null;
        for (FieldItem fieldItem : schemaItem.getSelectFields().values()) {
            Object value = getValFromRS(mapping, resultSet, fieldItem.getFieldName(), fieldItem.getFieldName());

            if (fieldItem.getFieldName().equals(idFieldName)) {
                resultIdVal = value;
                break;
            }
        }
        return resultIdVal;
    }

    public Object getESDataFromRS(ESMapping mapping, ResultSet resultSet, Map<String, Object> dmlOld,
                                  Map<String, Object> esFieldData) throws SQLException {
        SchemaItem schemaItem = mapping.getSchemaItem();
        String idFieldName = mapping.get_id() == null ? mapping.getPk() : mapping.get_id();
        Object resultIdVal = null;
        for (FieldItem fieldItem : schemaItem.getSelectFields().values()) {
            if (fieldItem.getFieldName().equals(idFieldName)) {
                resultIdVal = getValFromRS(mapping, resultSet, fieldItem.getFieldName(), fieldItem.getFieldName());
            }

            for (ColumnItem columnItem : fieldItem.getColumnItems()) {
                if (dmlOld.containsKey(columnItem.getColumnName())
                    && !mapping.getSkips().contains(fieldItem.getFieldName())) {
                    esFieldData.put(fieldItem.getFieldName(),
                        getValFromRS(mapping, resultSet, fieldItem.getFieldName(), fieldItem.getFieldName()));
                    break;
                }
            }
        }
        return resultIdVal;
    }

    public Object getValFromData(ESMapping mapping, Map<String, Object> dmlData, String fieldName, String columnName) {
        String esType = getEsType(mapping, fieldName);
        Object value = dmlData.get(columnName);
        if (value instanceof Byte) {
            if ("boolean".equals(esType)) {
                value = ((Byte) value).intValue() != 0;
            }
        }

        // 如果是对象类型
        if (mapping.getObjFields().containsKey(fieldName)) {
            return ESSyncUtil.convertToEsObj(value, mapping.getObjFields().get(fieldName));
        } else {
            return ESSyncUtil.typeConvert(value, esType);
        }
    }

    /**
     * 将dml的data转换为es的data
     *
     * @param mapping 配置mapping
     * @param dmlData dml data
     * @param esFieldData es data
     * @return 返回 id 值
     */
    public Object getESDataFromDmlData(ESMapping mapping, Map<String, Object> dmlData,
                                       Map<String, Object> esFieldData) {
        SchemaItem schemaItem = mapping.getSchemaItem();
        String idFieldName = mapping.get_id() == null ? mapping.getPk() : mapping.get_id();
        Object resultIdVal = null;
        for (FieldItem fieldItem : schemaItem.getSelectFields().values()) {
            String columnName = fieldItem.getColumnItems().iterator().next().getColumnName();
            Object value = getValFromData(mapping, dmlData, fieldItem.getFieldName(), columnName);

            if (fieldItem.getFieldName().equals(idFieldName)) {
                resultIdVal = value;
            }

            if (!fieldItem.getFieldName().equals(mapping.get_id())
                && !mapping.getSkips().contains(fieldItem.getFieldName())) {
                esFieldData.put(fieldItem.getFieldName(), value);
            }
        }
        return resultIdVal;
    }

    /**
     * 将dml的data, old转换为es的data
     *
     * @param mapping 配置mapping
     * @param dmlData dml data
     * @param esFieldData es data
     * @return 返回 id 值
     */
    public Object getESDataFromDmlData(ESMapping mapping, Map<String, Object> dmlData, Map<String, Object> dmlOld,
                                       Map<String, Object> esFieldData) {
        SchemaItem schemaItem = mapping.getSchemaItem();
        String idFieldName = mapping.get_id() == null ? mapping.getPk() : mapping.get_id();
        Object resultIdVal = null;
        for (FieldItem fieldItem : schemaItem.getSelectFields().values()) {
            String columnName = fieldItem.getColumnItems().iterator().next().getColumnName();

            if (fieldItem.getFieldName().equals(idFieldName)) {
                resultIdVal = getValFromData(mapping, dmlData, fieldItem.getFieldName(), columnName);
            }

            if (dmlOld.get(columnName) != null && !mapping.getSkips().contains(fieldItem.getFieldName())) {
                esFieldData.put(fieldItem.getFieldName(),
                    getValFromData(mapping, dmlData, fieldItem.getFieldName(), columnName));
            }
        }
        return resultIdVal;
    }

    /**
     * es 字段类型本地缓存
     */
    private static ConcurrentMap<String, Map<String, String>> esFieldTypes = new ConcurrentHashMap<>();

    /**
     * 获取es mapping中的属性类型
     *
     * @param mapping mapping配置
     * @param fieldName 属性名
     * @return 类型
     */
    @SuppressWarnings("unchecked")
    private String getEsType(ESMapping mapping, String fieldName) {
        String key = mapping.get_index() + "-" + mapping.get_type();
        Map<String, String> fieldType = esFieldTypes.get(key);
        if (fieldType == null) {
            ImmutableOpenMap<String, MappingMetaData> mappings;
            try {
                mappings = transportClient.admin()
                    .cluster()
                    .prepareState()
                    .execute()
                    .actionGet()
                    .getState()
                    .getMetaData()
                    .getIndices()
                    .get(mapping.get_index())
                    .getMappings();
            } catch (NullPointerException e) {
                throw new IllegalArgumentException("Not found the mapping info of index: " + mapping.get_index());
            }
            MappingMetaData mappingMetaData = mappings.get(mapping.get_type());
            if (mappingMetaData == null) {
                throw new IllegalArgumentException("Not found the mapping info of index: " + mapping.get_index());
            }

            fieldType = new LinkedHashMap<>();

            Map<String, Object> sourceMap = mappingMetaData.getSourceAsMap();
            Map<String, Object> esMapping = (Map<String, Object>) sourceMap.get("properties");
            for (Map.Entry<String, Object> entry : esMapping.entrySet()) {
                Map<String, Object> value = (Map<String, Object>) entry.getValue();
                if (value.containsKey("properties")) {
                    fieldType.put(entry.getKey(), "object");
                } else {
                    fieldType.put(entry.getKey(), (String) value.get("type"));
                }
            }
            esFieldTypes.put(key, fieldType);
        }

        return fieldType.get(fieldName);
    }
}
