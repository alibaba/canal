package com.alibaba.otter.canal.client.adapter.es.service;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig.ESMapping;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem.FieldItem;
import com.alibaba.otter.canal.client.adapter.es.support.ESTemplate;
import com.alibaba.otter.canal.client.adapter.support.AbstractEtlService;
import com.alibaba.otter.canal.client.adapter.support.AdapterConfig;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.Util;

/**
 * ES ETL Service
 *
 * @author rewerma 2018-11-01
 * @version 1.0.0
 */
public class ESEtlService extends AbstractEtlService {

    private TransportClient transportClient;
    private ESTemplate      esTemplate;
    private ESSyncConfig    config;

    public ESEtlService(TransportClient transportClient, ESSyncConfig config){
        super("ES", config);
        this.transportClient = transportClient;
        this.esTemplate = new ESTemplate(transportClient);
        this.config = config;
    }

    public EtlResult importData(List<String> params) {
        ESMapping mapping = config.getEsMapping();
        logger.info("start etl to import data to index: {}", mapping.get_index());
        String sql = mapping.getSql();
        return importData(sql, params);
    }

    private void processFailBulkResponse(BulkResponse bulkResponse) {
        for (BulkItemResponse response : bulkResponse.getItems()) {
            if (!response.isFailed()) {
                continue;
            }

            if (response.getFailure().getStatus() == RestStatus.NOT_FOUND) {
                logger.warn(response.getFailureMessage());
            } else {
                logger.error("全量导入数据有误 {}", response.getFailureMessage());
                throw new RuntimeException("全量数据 etl 异常: " + response.getFailureMessage());
            }
        }
    }

    protected boolean executeSqlImport(DataSource ds, String sql, List<Object> values,
                                       AdapterConfig.AdapterMapping adapterMapping, AtomicLong impCount,
                                       List<String> errMsg) {
        try {
            ESMapping mapping = (ESMapping) adapterMapping;
            Util.sqlRS(ds, sql, values, rs -> {
                int count = 0;
                try {
                    BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();

                    long batchBegin = System.currentTimeMillis();
                    while (rs.next()) {
                        Map<String, Object> esFieldData = new LinkedHashMap<>();
                        Object idVal = null;
                        for (FieldItem fieldItem : mapping.getSchemaItem().getSelectFields().values()) {

                            String fieldName = fieldItem.getFieldName();
                            if (mapping.getSkips().contains(fieldName)) {
                                continue;
                            }

                            // 如果是主键字段则不插入
                            if (fieldItem.getFieldName().equals(mapping.get_id())) {
                                idVal = esTemplate.getValFromRS(mapping, rs, fieldName, fieldName);
                            } else {
                                Object val = esTemplate.getValFromRS(mapping, rs, fieldName, fieldName);
                                esFieldData.put(Util.cleanColumn(fieldName), val);
                            }

                        }

                        if (!mapping.getRelations().isEmpty()) {
                            mapping.getRelations().forEach((relationField, relationMapping) -> {
                                Map<String, Object> relations = new HashMap<>();
                                relations.put("name", relationMapping.getName());
                                if (StringUtils.isNotEmpty(relationMapping.getParent())) {
                                    FieldItem parentFieldItem = mapping.getSchemaItem()
                                        .getSelectFields()
                                        .get(relationMapping.getParent());
                                    Object parentVal;
                                    try {
                                        parentVal = esTemplate.getValFromRS(mapping,
                                            rs,
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
                                esFieldData.put(Util.cleanColumn(relationField), relations);
                            });
                        }

                        if (idVal != null) {
                            String parentVal = (String) esFieldData.remove("$parent_routing");
                            if (mapping.isUpsert()) {
                                UpdateRequestBuilder updateRequestBuilder = transportClient
                                    .prepareUpdate(mapping.get_index(), mapping.get_type(), idVal.toString())
                                    .setDoc(esFieldData)
                                    .setDocAsUpsert(true);
                                if (StringUtils.isNotEmpty(parentVal)) {
                                    updateRequestBuilder.setRouting(parentVal);
                                }
                                bulkRequestBuilder.add(updateRequestBuilder);
                            } else {
                                IndexRequestBuilder indexRequestBuilder = transportClient
                                    .prepareIndex(mapping.get_index(), mapping.get_type(), idVal.toString())
                                    .setSource(esFieldData);
                                if (StringUtils.isNotEmpty(parentVal)) {
                                    indexRequestBuilder.setRouting(parentVal);
                                }
                                bulkRequestBuilder.add(indexRequestBuilder);
                            }
                        } else {
                            idVal = esFieldData.get(mapping.getPk());
                            SearchResponse response = transportClient.prepareSearch(mapping.get_index())
                                .setTypes(mapping.get_type())
                                .setQuery(QueryBuilders.termQuery(mapping.getPk(), idVal))
                                .setSize(10000)
                                .get();
                            for (SearchHit hit : response.getHits()) {
                                bulkRequestBuilder.add(
                                    transportClient.prepareUpdate(mapping.get_index(), mapping.get_type(), hit.getId())
                                        .setDoc(esFieldData));
                            }
                        }

                        if (bulkRequestBuilder.numberOfActions() % mapping.getCommitBatch() == 0
                            && bulkRequestBuilder.numberOfActions() > 0) {
                            long esBatchBegin = System.currentTimeMillis();
                            BulkResponse rp = bulkRequestBuilder.execute().actionGet();
                            if (rp.hasFailures()) {
                                this.processFailBulkResponse(rp);
                            }

                            if (logger.isTraceEnabled()) {
                                logger.trace("全量数据批量导入批次耗时: {}, es执行时间: {}, 批次大小: {}, index; {}",
                                    (System.currentTimeMillis() - batchBegin),
                                    (System.currentTimeMillis() - esBatchBegin),
                                    bulkRequestBuilder.numberOfActions(),
                                    mapping.get_index());
                            }
                            batchBegin = System.currentTimeMillis();
                            bulkRequestBuilder = transportClient.prepareBulk();
                        }
                        count++;
                        impCount.incrementAndGet();
                    }

                    if (bulkRequestBuilder.numberOfActions() > 0) {
                        long esBatchBegin = System.currentTimeMillis();
                        BulkResponse rp = bulkRequestBuilder.execute().actionGet();
                        if (rp.hasFailures()) {
                            this.processFailBulkResponse(rp);
                        }
                        if (logger.isTraceEnabled()) {
                            logger.trace("全量数据批量导入最后批次耗时: {}, es执行时间: {}, 批次大小: {}, index; {}",
                                (System.currentTimeMillis() - batchBegin),
                                (System.currentTimeMillis() - esBatchBegin),
                                bulkRequestBuilder.numberOfActions(),
                                mapping.get_index());
                        }
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    errMsg.add(mapping.get_index() + " etl failed! ==>" + e.getMessage());
                    throw new RuntimeException(e);
                }
                return count;
            });

            return true;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        }
    }
}
