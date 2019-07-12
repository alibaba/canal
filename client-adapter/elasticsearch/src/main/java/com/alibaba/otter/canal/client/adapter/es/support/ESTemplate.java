package com.alibaba.otter.canal.client.adapter.es.support;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig.ESMapping;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem.ColumnItem;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem.FieldItem;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.Util;

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

    private BulkRequestBuilder  bulkRequestBuilder;

    public ESTemplate(TransportClient transportClient){
        this.transportClient = transportClient;
        this.bulkRequestBuilder = transportClient.prepareBulk();
    }

    public BulkRequestBuilder getBulk() {
        return bulkRequestBuilder;
    }

    public void resetBulkRequestBuilder() {
        this.bulkRequestBuilder = this.transportClient.prepareBulk();
    }

    /**
     * 插入数据
     *
     * @param mapping 配置对象
     * @param pkVal 主键值
     * @param esFieldData 数据Map
     */
    public void insert(ESMapping mapping, Object pkVal, Map<String, Object> esFieldData) {
        if (mapping.get_id() != null) {
            String parentVal = (String) esFieldData.remove("$parent_routing");
            if (mapping.isUpsert()) {
                UpdateRequestBuilder updateRequestBuilder = transportClient
                    .prepareUpdate(mapping.get_index(), mapping.get_type(), pkVal.toString())
                    .setDoc(esFieldData)
                    .setDocAsUpsert(true);
                if (StringUtils.isNotEmpty(parentVal)) {
                    updateRequestBuilder.setRouting(parentVal);
                }
                getBulk().add(updateRequestBuilder);
            } else {
                IndexRequestBuilder indexRequestBuilder = transportClient
                    .prepareIndex(mapping.get_index(), mapping.get_type(), pkVal.toString())
                    .setSource(esFieldData);
                if (StringUtils.isNotEmpty(parentVal)) {
                    indexRequestBuilder.setRouting(parentVal);
                }
                getBulk().add(indexRequestBuilder);
            }
            commitBulk();
        } else {
            SearchResponse response = transportClient.prepareSearch(mapping.get_index())
                .setTypes(mapping.get_type())
                .setQuery(QueryBuilders.termQuery(mapping.getPk(), pkVal))
                .setSize(10000)
                .get();
            for (SearchHit hit : response.getHits()) {
                getBulk().add(transportClient.prepareUpdate(mapping.get_index(), mapping.get_type(), hit.getId())
                    .setDoc(esFieldData));
                commitBulk();
            }
        }

    }

    /**
     * 根据主键更新数据
     *
     * @param mapping 配置对象
     * @param pkVal 主键值
     * @param esFieldData 数据Map
     */
    public void update(ESMapping mapping, Object pkVal, Map<String, Object> esFieldData) {
        Map<String, Object> esFieldDataTmp = new LinkedHashMap<>(esFieldData.size());
        esFieldData.forEach((k, v) -> esFieldDataTmp.put(Util.cleanColumn(k), v));
        append4Update(mapping, pkVal, esFieldDataTmp);
        commitBulk();
    }

    /**
     * update by query
     *
     * @param config 配置对象
     * @param paramsTmp sql查询条件
     * @param esFieldData 数据Map
     */
    public void updateByQuery(ESSyncConfig config, Map<String, Object> paramsTmp, Map<String, Object> esFieldData) {
        if (paramsTmp.isEmpty()) {
            return;
        }
        ESMapping mapping = config.getEsMapping();
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        paramsTmp.forEach((fieldName, value) -> queryBuilder.must(QueryBuilders.termsQuery(fieldName, value)));

        // 查询sql批量更新
        DataSource ds = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        StringBuilder sql = new StringBuilder("SELECT * FROM (" + mapping.getSql() + ") _v WHERE ");
        List<Object> values = new ArrayList<>();
        paramsTmp.forEach((fieldName, value) -> {
            sql.append("_v.").append(fieldName).append("=? AND ");
            values.add(value);
        });
        //TODO 直接外部包裹sql会导致全表扫描性能低, 待优化拼接内部where条件
        int len = sql.length();
        sql.delete(len - 4, len);
        Integer syncCount = (Integer) Util.sqlRS(ds, sql.toString(), values, rs -> {
            int count = 0;
            try {
                while (rs.next()) {
                    Object idVal = getIdValFromRS(mapping, rs);
                    append4Update(mapping, idVal, esFieldData);
                    commitBulk();
                    count++;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return count;
        });
        if (logger.isTraceEnabled()) {
            logger.trace("Update ES by query affected {} records", syncCount);
        }
    }

    /**
     * 通过主键删除数据
     *
     * @param mapping 配置对象
     * @param pkVal 主键值
     * @param esFieldData 数据Map
     */
    public void delete(ESMapping mapping, Object pkVal, Map<String, Object> esFieldData) {
        if (mapping.get_id() != null) {
            getBulk().add(transportClient.prepareDelete(mapping.get_index(), mapping.get_type(), pkVal.toString()));
            commitBulk();
        } else {
            SearchResponse response = transportClient.prepareSearch(mapping.get_index())
                .setTypes(mapping.get_type())
                .setQuery(QueryBuilders.termQuery(mapping.getPk(), pkVal))
                .setSize(10000)
                .get();
            for (SearchHit hit : response.getHits()) {
                getBulk().add(transportClient.prepareUpdate(mapping.get_index(), mapping.get_type(), hit.getId())
                    .setDoc(esFieldData));
                commitBulk();
            }
        }

    }

    /**
     * 提交批次
     */
    public void commit() {
        if (getBulk().numberOfActions() > 0) {
            BulkResponse response = getBulk().execute().actionGet();
            if (response.hasFailures()) {
                for (BulkItemResponse itemResponse : response.getItems()) {
                    if (!itemResponse.isFailed()) {
                        continue;
                    }

                    if (itemResponse.getFailure().getStatus() == RestStatus.NOT_FOUND) {
                        logger.error(itemResponse.getFailureMessage());
                    } else {
                        throw new RuntimeException("ES sync commit error" + itemResponse.getFailureMessage());
                    }
                }
            }
            resetBulkRequestBuilder();
        }
    }

    /**
     * 如果大于批量数则提交批次
     */
    private void commitBulk() {
        if (getBulk().numberOfActions() >= MAX_BATCH_SIZE) {
            commit();
        }
    }

    private void append4Update(ESMapping mapping, Object pkVal, Map<String, Object> esFieldData) {
        if (mapping.get_id() != null) {
            String parentVal = (String) esFieldData.remove("$parent_routing");
            if (mapping.isUpsert()) {
                UpdateRequestBuilder updateRequestBuilder = transportClient
                    .prepareUpdate(mapping.get_index(), mapping.get_type(), pkVal.toString())
                    .setDoc(esFieldData)
                    .setDocAsUpsert(true);
                if (StringUtils.isNotEmpty(parentVal)) {
                    updateRequestBuilder.setRouting(parentVal);
                }
                getBulk().add(updateRequestBuilder);
            } else {
                UpdateRequestBuilder updateRequestBuilder = transportClient
                    .prepareUpdate(mapping.get_index(), mapping.get_type(), pkVal.toString())
                    .setDoc(esFieldData);
                if (StringUtils.isNotEmpty(parentVal)) {
                    updateRequestBuilder.setRouting(parentVal);
                }
                getBulk().add(updateRequestBuilder);
            }
        } else {
            SearchResponse response = transportClient.prepareSearch(mapping.get_index())
                .setTypes(mapping.get_type())
                .setQuery(QueryBuilders.termQuery(mapping.getPk(), pkVal))
                .setSize(10000)
                .get();
            for (SearchHit hit : response.getHits()) {
                getBulk().add(transportClient.prepareUpdate(mapping.get_index(), mapping.get_type(), hit.getId())
                    .setDoc(esFieldData));
            }
        }
    }

    public Object getValFromRS(ESMapping mapping, ResultSet resultSet, String fieldName,
                               String columnName) throws SQLException {
        fieldName = Util.cleanColumn(fieldName);
        columnName = Util.cleanColumn(columnName);
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
                esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), value);
            }
        }

        // 添加父子文档关联信息
        putRelationDataFromRS(mapping, schemaItem, resultSet, esFieldData);

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
                    esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()),
                        getValFromRS(mapping, resultSet, fieldItem.getFieldName(), fieldItem.getFieldName()));
                    break;
                }
            }
        }

        // 添加父子文档关联信息
        putRelationDataFromRS(mapping, schemaItem, resultSet, esFieldData);

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
                esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), value);
            }
        }

        // 添加父子文档关联信息
        putRelationData(mapping, schemaItem, dmlData, esFieldData);
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

            if (dmlOld.containsKey(columnName) && !mapping.getSkips().contains(fieldItem.getFieldName())) {
                esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()),
                    getValFromData(mapping, dmlData, fieldItem.getFieldName(), columnName));
            }
        }

        // 添加父子文档关联信息
        putRelationData(mapping, schemaItem, dmlOld, esFieldData);
        return resultIdVal;
    }

    private void putRelationDataFromRS(ESMapping mapping, SchemaItem schemaItem, ResultSet resultSet,
                                       Map<String, Object> esFieldData) {
        // 添加父子文档关联信息
        if (!mapping.getRelations().isEmpty()) {
            mapping.getRelations().forEach((relationField, relationMapping) -> {
                Map<String, Object> relations = new HashMap<>();
                relations.put("name", relationMapping.getName());
                if (StringUtils.isNotEmpty(relationMapping.getParent())) {
                    FieldItem parentFieldItem = schemaItem.getSelectFields().get(relationMapping.getParent());
                    Object parentVal;
                    try {
                        parentVal = getValFromRS(mapping,
                            resultSet,
                            parentFieldItem.getFieldName(),
                            parentFieldItem.getFieldName());
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                    if (parentVal != null) {
                        relations.put("parent", parentVal.toString());
                        esFieldData.put("$parent_routing", parentVal.toString());

                    }
                }
                esFieldData.put(relationField, relations);
            });
        }
    }

    private void putRelationData(ESMapping mapping, SchemaItem schemaItem, Map<String, Object> dmlData,
                                 Map<String, Object> esFieldData) {
        // 添加父子文档关联信息
        if (!mapping.getRelations().isEmpty()) {
            mapping.getRelations().forEach((relationField, relationMapping) -> {
                Map<String, Object> relations = new HashMap<>();
                relations.put("name", relationMapping.getName());
                if (StringUtils.isNotEmpty(relationMapping.getParent())) {
                    FieldItem parentFieldItem = schemaItem.getSelectFields().get(relationMapping.getParent());
                    String columnName = parentFieldItem.getColumnItems().iterator().next().getColumnName();
                    Object parentVal = getValFromData(mapping, dmlData, parentFieldItem.getFieldName(), columnName);
                    if (parentVal != null) {
                        relations.put("parent", parentVal.toString());
                        esFieldData.put("$parent_routing", parentVal.toString());

                    }
                }
                esFieldData.put(relationField, relations);
            });
        }
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
