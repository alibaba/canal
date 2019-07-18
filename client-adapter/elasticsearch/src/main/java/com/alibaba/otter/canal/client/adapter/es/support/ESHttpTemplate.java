package com.alibaba.otter.canal.client.adapter.es.support;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig.ESMapping;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem.ColumnItem;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem.FieldItem;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.Util;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * ES 操作模板
 *
 * @author rewerma 2018-11-01
 * @version 1.0.0
 */
public class ESHttpTemplate {

    private static final Logger logger         = LoggerFactory.getLogger(ESHttpTemplate.class);

    private static final int    MAX_BATCH_SIZE = 1000;

    private RestHighLevelClient     restHighLevelClient;

    private BulkRequest bulkRequest;

    public ESHttpTemplate(RestHighLevelClient restHighLevelClient){
        this.restHighLevelClient = restHighLevelClient;
        this.bulkRequest = new BulkRequest();
    }

    public BulkRequest getBulk() {
        return bulkRequest;
    }

    /**
     * 插入数据
     *
     * @param mapping 配置对象
     * @param pkVal 主键值
     * @param esFieldData 数据Map
     */
    public void insert(ESMapping mapping, Object pkVal, Map<String, Object> esFieldData) throws IOException {
        if (mapping.get_id() != null) {
            String parentVal = (String) esFieldData.remove("$parent_routing");
            if (mapping.isUpsert()) {
                UpdateRequest updateRequest = new UpdateRequest(mapping.get_index(), mapping.get_type(), pkVal.toString())
                                                    .doc(esFieldData)
                                                    .docAsUpsert(true);
                if (StringUtils.isNotEmpty(parentVal)) {
                    updateRequest.routing(parentVal);
                }
                getBulk().add(updateRequest);
            } else {
                IndexRequest indexRequest = new IndexRequest(mapping.get_index(), mapping.get_type(), pkVal.toString())
                        .source(esFieldData);
                if (StringUtils.isNotEmpty(parentVal)) {
                    indexRequest.routing(parentVal);
                }
                getBulk().add(indexRequest);
            }
            commitBulk();
        } else {
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(QueryBuilders.termQuery(mapping.getPk(), pkVal))
                    .size(10000);
            SearchRequest searchRequest = new SearchRequest(mapping.get_index())
                    .types(mapping.get_type())
                    .source(sourceBuilder);
            SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            for (SearchHit hit : response.getHits()) {
                UpdateRequest updateRequest = new UpdateRequest(mapping.get_index(), mapping.get_type(), hit.getId())
                        .doc(esFieldData);
                getBulk().add(updateRequest);
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
    public void update(ESMapping mapping, Object pkVal, Map<String, Object> esFieldData) throws IOException {
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
        paramsTmp.forEach(
            (fieldName, value) -> sql.append("_v.").append(fieldName).append("=").append(value).append(" AND "));
        int len = sql.length();
        sql.delete(len - 4, len);
        Integer syncCount = (Integer) Util.sqlRS(ds, sql.toString(), rs -> {
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
    public void delete(ESMapping mapping, Object pkVal, Map<String, Object> esFieldData) throws IOException {
        if (mapping.get_id() != null) {
            DeleteRequest deleteRequest = new DeleteRequest(mapping.get_index(), mapping.get_type(), pkVal.toString());
            getBulk().add(deleteRequest);
            commitBulk();
        } else {
            SearchSourceBuilder builder = new SearchSourceBuilder();
            builder.query(QueryBuilders.termQuery(mapping.getPk(), pkVal))
                    .size(10000);
            SearchRequest searchRequest = new SearchRequest(mapping.get_index())
                    .types(mapping.get_type())
                    .source(builder);
            SearchResponse response = restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);
            for (SearchHit hit : response.getHits()) {
                UpdateRequest updateRequest = new UpdateRequest(mapping.get_index(), mapping.get_type(), hit.getId());
                updateRequest.doc(esFieldData);
                getBulk().add(updateRequest);
                commitBulk();
            }
        }

    }

    /**
     * 提交批次
     */
    public void commit() throws IOException {
        if (getBulk().numberOfActions() > 0) {
            BulkResponse response = restHighLevelClient.bulk(getBulk(), RequestOptions.DEFAULT);
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
        }
    }

    /**
     * 如果大于批量数则提交批次
     */
    private void commitBulk() throws IOException {
        if (getBulk().numberOfActions() >= MAX_BATCH_SIZE) {
            commit();
        }
    }

    private void append4Update(ESMapping mapping, Object pkVal, Map<String, Object> esFieldData) throws IOException {
        if (mapping.get_id() != null) {
            String parentVal = (String) esFieldData.remove("$parent_routing");
            if (mapping.isUpsert()) {
                UpdateRequest updateRequest = new UpdateRequest(mapping.get_index(), mapping.get_type(), pkVal.toString());
                updateRequest.doc(esFieldData).docAsUpsert(true);
                if (StringUtils.isNotEmpty(parentVal)) {
                    updateRequest.routing(parentVal);
                }
                getBulk().add(updateRequest);
            } else {
                UpdateRequest updateRequest = new UpdateRequest(mapping.get_index(), mapping.get_type(), pkVal.toString());
                updateRequest.doc(esFieldData);
                if (StringUtils.isNotEmpty(parentVal)) {
                    updateRequest.routing(parentVal);
                }
                getBulk().add(updateRequest);
            }
        } else {
            SearchSourceBuilder builder = new SearchSourceBuilder();
            builder.query(QueryBuilders.termQuery(mapping.getPk(), pkVal))
                    .size(10000);
            SearchRequest searchRequest = new SearchRequest(mapping.get_index())
                    .types(mapping.get_type())
                    .source(builder);
            SearchResponse response = restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);
            for (SearchHit hit : response.getHits()) {
                UpdateRequest updateRequest = new UpdateRequest(mapping.get_index(), mapping.get_type(), hit.getId());
                updateRequest.doc(esFieldData);
                getBulk().add(updateRequest);
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

    public Object getIdValFromRS(ESMapping mapping, ResultSet resultSet) throws SQLException, IOException {
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
                                  Map<String, Object> esFieldData) throws SQLException, IOException {
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
            Map<String, MappingMetaData> mappings;
            try {
                GetMappingsRequest request = new GetMappingsRequest();
                request.indices(mapping.get_index());
                GetMappingsResponse response = restHighLevelClient.indices().getMapping(request, RequestOptions.DEFAULT);
                mappings = response.mappings();
            } catch (NullPointerException e) {
                throw new IllegalArgumentException("Not found the mapping info of index: " + mapping.get_index());
            } catch (IOException e) {
                logger.error(e.getMessage(),e);
                return null;
            }
            MappingMetaData mappingMetaData = mappings.get(mapping.get_index());
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
