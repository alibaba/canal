package com.alibaba.otter.canal.client.adapter.es6x.etl;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.adapter.es.core.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.core.config.ESSyncConfig.ESMapping;
import com.alibaba.otter.canal.client.adapter.es.core.config.SchemaItem.FieldItem;
import com.alibaba.otter.canal.client.adapter.es.core.service.ESSyncService;
import com.alibaba.otter.canal.client.adapter.es.core.support.ESBulkRequest;
import com.alibaba.otter.canal.client.adapter.es.core.support.ESBulkRequest.ESBulkResponse;
import com.alibaba.otter.canal.client.adapter.es.core.support.ESBulkRequest.ESIndexRequest;
import com.alibaba.otter.canal.client.adapter.es.core.support.ESBulkRequest.ESUpdateRequest;
import com.alibaba.otter.canal.client.adapter.es.core.support.ESBulkRequest.IESRequest;
import com.alibaba.otter.canal.client.adapter.es.core.support.ESTemplate;
import com.alibaba.otter.canal.client.adapter.es6x.support.ES6xTemplate;
import com.alibaba.otter.canal.client.adapter.es6x.support.ESConnection;
import com.alibaba.otter.canal.client.adapter.es6x.support.ESConnection.ESSearchRequest;
import com.alibaba.otter.canal.client.adapter.support.AbstractEtlService;
import com.alibaba.otter.canal.client.adapter.support.AdapterConfig;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.Util;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ES ETL Service
 *
 * @author rewerma 2018-11-01
 * @version 1.0.0
 */
public class ESEtlService extends AbstractEtlService {

    private ESConnection esConnection;
    private ESTemplate   esTemplate;
    private ESSyncConfig config;
    private ESSyncService esSyncService;

    public ESEtlService(ESConnection esConnection, ESSyncConfig config, ESSyncService esSyncService){
        super("ES", config);
        this.esConnection = esConnection;
        this.esTemplate = new ES6xTemplate(esConnection);
        this.config = config;
        this.esSyncService = esSyncService;
    }

    public EtlResult importData(List<String> params) {
        ESMapping mapping = config.getEsMapping();
        logger.info("start etl to import data to index: {}", mapping.get_index());
        String sql = mapping.getSql();
        return importData(sql, params);
    }

    protected boolean executeSqlImport(DataSource ds, String sql, List<Object> values,
                                       AdapterConfig.AdapterMapping adapterMapping, AtomicLong impCount,
                                       List<String> errMsg) {
        try {
            ESMapping mapping = (ESMapping) adapterMapping;
            Util.sqlRS(ds, sql, values, rs -> {
                int count = 0;
                try {
                    ESBulkRequest esBulkRequest = this.esConnection.new ES6xBulkRequest();

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

                        //填充对象字段值
                        esFieldData.putAll(esSyncService.getObjectFieldDatasForMainTableInsert(config, esFieldData));

                        if (idVal != null) {
                            String parentVal = (String) esFieldData.remove("$parent_routing");
                            if (mapping.isUpsert()) {
                                ESUpdateRequest esUpdateRequest = this.esConnection.new ES6xUpdateRequest(mapping.get_index(),
                                        mapping.get_type(),
                                        idVal.toString()).setDoc(esFieldData).setDocAsUpsert(true);

                                if (StringUtils.isNotEmpty(parentVal)) {
                                    esUpdateRequest.setRouting(parentVal);
                                }

                                batchBegin = add(mapping, esBulkRequest, esFieldData, esUpdateRequest, batchBegin);
                            } else {
                                ESIndexRequest esIndexRequest = this.esConnection.new ES6xIndexRequest(mapping.get_index(),
                                        mapping.get_type(),
                                        idVal.toString()).setSource(esFieldData);
                                if (StringUtils.isNotEmpty(parentVal)) {
                                    esIndexRequest.setRouting(parentVal);
                                }
                                batchBegin = add(mapping, esBulkRequest, esFieldData, esIndexRequest, batchBegin);
                            }
                        } else {
                            idVal = esFieldData.get(mapping.getPk());
                            ESSearchRequest esSearchRequest = this.esConnection.new ESSearchRequest(mapping.get_index(),
                                    mapping.get_type()).setQuery(QueryBuilders.termQuery(mapping.getPk(), idVal)).size(10000);
                            SearchResponse response = esSearchRequest.getResponse();
                            for (SearchHit hit : response.getHits()) {
                                ESUpdateRequest esUpdateRequest = this.esConnection.new ES6xUpdateRequest(mapping.get_index(),
                                        mapping.get_type(),
                                        hit.getId()).setDoc(esFieldData);
                                batchBegin = add(mapping, esBulkRequest, esFieldData, esUpdateRequest, batchBegin);
                            }
                        }

                        if (esBulkRequest.numberOfActions() % mapping.getCommitBatch() == 0 && esBulkRequest.numberOfActions() > 0) {
                            bulk(mapping, esBulkRequest, batchBegin, false);
                            batchBegin = System.currentTimeMillis();
                        }
                        count++;
                        impCount.incrementAndGet();
                    }

                    if (esBulkRequest.numberOfActions() > 0) {
                        bulk(mapping, esBulkRequest, batchBegin, true);
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


    private long add(ESMapping mapping, ESBulkRequest esBulkRequest, Map<String, Object> esFieldData, IESRequest esRequest, long batchBegin) {
        boolean leCommitBatchSize =
                esBulkRequest.add(esRequest, mapping.getCommitBatchSize(), bytesSizeToAdd -> {
                    if (esBulkRequest.numberOfActions() <= 0) {
                        try {
                            esBulkRequest.add(esRequest);
                            bulk(mapping, esBulkRequest, batchBegin, false);
                        } catch (Exception e) {
                            logger.error("全量数据批量导入批次中单条数据已达上限, 尝试单独推送失败！" + JSON.toJSONString(esFieldData), e);
                            throw e;
                        }
                    } else {
                        //若批次数据大小已达上限，不继续添加，先提交再手动添加

                        bulk(mapping, esBulkRequest, batchBegin, false);
                        esBulkRequest.add(esRequest);
                    }
                    return false;
                });
        if (leCommitBatchSize) {
            return batchBegin;
        }
        return System.currentTimeMillis();
    }

    private void bulk(ESMapping mapping, ESBulkRequest esBulkRequest, long batchBegin, boolean isLast) {
        int numberOfActions = esBulkRequest.numberOfActions();
        if (numberOfActions <= 0) {
            logger.warn("全量数据批量导入批次无数据, 忽略");
            return;
        }

        try {
            long esBatchBegin = System.currentTimeMillis();
            ESBulkResponse rp = esBulkRequest.bulk();
            if (rp.hasFailures()) {
                rp.processFailBulkResponse("全量数据 etl 异常 ");
            }

            if (logger.isTraceEnabled()) {
                logger.trace("全量数据批量导入{}批次耗时: {}, es执行时间: {}, 批次个数: {}, 批次大小: {}, index; {}",
                        isLast ? "最后" : "",
                        (System.currentTimeMillis() - batchBegin),
                        (System.currentTimeMillis() - esBatchBegin),
                        esBulkRequest.numberOfActions(),
                        esBulkRequest.estimatedSizeInBytes(),
                        mapping.get_index());
            }
        } finally {
            esBulkRequest.resetBulk();
        }
    }
}
