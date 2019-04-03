package com.alibaba.otter.canal.client.adapter.es.service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig.ESMapping;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem.FieldItem;
import com.alibaba.otter.canal.client.adapter.es.support.ESTemplate;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.Util;
import com.google.common.base.Joiner;

/**
 * ES ETL Service
 *
 * @author rewerma 2018-11-01
 * @version 1.0.0
 */
public class ESEtlService {

    private static Logger   logger = LoggerFactory.getLogger(ESEtlService.class);

    private TransportClient transportClient;
    private ESTemplate      esTemplate;
    private ESSyncConfig    config;

    public ESEtlService(TransportClient transportClient, ESSyncConfig config){
        this.transportClient = transportClient;
        this.esTemplate = new ESTemplate(transportClient);
        this.config = config;
    }

    public EtlResult importData(List<String> params) {
        EtlResult etlResult = new EtlResult();
        AtomicLong impCount = new AtomicLong();
        List<String> errMsg = new ArrayList<>();
        String esIndex = "";
        if (config == null) {
            logger.warn("esSycnCofnig is null, etl go end !");
            etlResult.setErrorMessage("esSycnCofnig is null, etl go end !");
            return etlResult;
        }

        ESMapping mapping = config.getEsMapping();

        esIndex = mapping.get_index();
        DruidDataSource dataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        Pattern pattern = Pattern.compile(".*:(.*)://.*/(.*)\\?.*$");
        Matcher matcher = pattern.matcher(dataSource.getUrl());
        if (!matcher.find()) {
            throw new RuntimeException("Not found the schema of jdbc-url: " + config.getDataSourceKey());
        }
        String schema = matcher.group(2);

        logger.info("etl from db: {},  to es index: {}", schema, esIndex);
        long start = System.currentTimeMillis();
        try {
            String sql = mapping.getSql();

            // 拼接条件
            if (mapping.getEtlCondition() != null && params != null) {
                String etlCondition = mapping.getEtlCondition();
                int size = params.size();
                for (int i = 0; i < size; i++) {
                    etlCondition = etlCondition.replace("{" + i + "}", params.get(i));
                }

                sql += " " + etlCondition;
            }

            if (logger.isDebugEnabled()) {
                logger.debug("etl sql : {}", mapping.getSql());
            }

            // 获取总数
            String countSql = "SELECT COUNT(1) FROM ( " + sql + ") _CNT ";
            long cnt = (Long) Util.sqlRS(dataSource, countSql, rs -> {
                Long count = null;
                try {
                    if (rs.next()) {
                        count = ((Number) rs.getObject(1)).longValue();
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
                return count == null ? 0L : count;
            });

            // 当大于1万条记录时开启多线程
            if (cnt >= 10000) {
                int threadCount = 3; // 从配置读取默认为3
                long perThreadCnt = cnt / threadCount;
                ExecutorService executor = Util.newFixedThreadPool(threadCount, 5000L);
                for (int i = 0; i < threadCount; i++) {
                    long offset = i * perThreadCnt;
                    Long size = null;
                    if (i != threadCount - 1) {
                        size = perThreadCnt;
                    }
                    String sqlFinal;
                    if (size != null) {
                        sqlFinal = sql + " LIMIT " + offset + "," + size;
                    } else {
                        sqlFinal = sql + " LIMIT " + offset + "," + cnt;
                    }
                    executor.execute(() -> executeSqlImport(dataSource, sqlFinal, mapping, impCount, errMsg));
                }

                executor.shutdown();
                while (!executor.awaitTermination(3, TimeUnit.SECONDS)) {
                    // ignore
                }
            } else {
                executeSqlImport(dataSource, sql, mapping, impCount, errMsg);
            }

            logger.info("数据全量导入完成,一共导入 {} 条数据, 耗时: {}", impCount.get(), System.currentTimeMillis() - start);
            etlResult.setResultMessage("导入ES索引 " + esIndex + " 数据：" + impCount.get() + " 条");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            errMsg.add(esIndex + " etl failed! ==>" + e.getMessage());
        }
        if (errMsg.isEmpty()) {
            etlResult.setSucceeded(true);
        } else {
            etlResult.setErrorMessage(Joiner.on("\n").join(errMsg));
        }
        return etlResult;
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

    private boolean executeSqlImport(DataSource ds, String sql, ESMapping mapping, AtomicLong impCount,
                                     List<String> errMsg) {
        try {
            Util.sqlRS(ds, sql, rs -> {
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
