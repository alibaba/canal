package com.alibaba.otter.canal.client.adapter.es.service;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig.ESMapping;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem.FieldItem;
import com.alibaba.otter.canal.client.adapter.es.support.ESHttpTemplate;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.Util;
import com.google.common.base.Joiner;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ES ETL Service
 *
 * @author rewerma 2018-11-01
 * @version 1.0.0
 */
public class ESHttpEtlService {

    private static Logger   logger = LoggerFactory.getLogger(ESHttpEtlService.class);

    private RestHighLevelClient restHighLevelClient;
    private ESHttpTemplate esHttpTemplate;
    private ESSyncConfig    config;

    public ESHttpEtlService(RestHighLevelClient restHighLevelClient, ESSyncConfig config){
        this.restHighLevelClient = restHighLevelClient;
        this.esHttpTemplate = new ESHttpTemplate(restHighLevelClient);
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
//                    BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();
                    BulkRequest bulkRequest = new BulkRequest();
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
                                idVal = esHttpTemplate.getValFromRS(mapping, rs, fieldName, fieldName);
                            } else {
                                Object val = esHttpTemplate.getValFromRS(mapping, rs, fieldName, fieldName);
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
                                        parentVal = esHttpTemplate.getValFromRS(mapping,
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
                                UpdateRequest updateRequest = new UpdateRequest(mapping.get_index(), mapping.get_type(), idVal.toString());
                                updateRequest.doc(esFieldData).docAsUpsert(true);
                                if (StringUtils.isNotEmpty(parentVal)) {
                                    updateRequest.routing(parentVal);
                                }
                                bulkRequest.add(updateRequest);
                            } else {
                                IndexRequest indexRequest = new IndexRequest(mapping.get_index(), mapping.get_type(), idVal.toString());
                                indexRequest.source(esFieldData);
                                if (StringUtils.isNotEmpty(parentVal)) {
                                    indexRequest.routing(parentVal);
                                }
                                bulkRequest.add(indexRequest);
                            }
                        } else {
                            idVal = esFieldData.get(mapping.getPk());
                            SearchSourceBuilder builder = new SearchSourceBuilder();
                            builder.query(QueryBuilders.termQuery(mapping.getPk(), idVal))
                                    .size(10000);
                            SearchRequest searchRequest = new SearchRequest(mapping.get_index());
                            searchRequest.types(mapping.get_type())
                                    .source(builder);
                            SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
                            for (SearchHit hit : response.getHits()) {
                                UpdateRequest updateRequest = new UpdateRequest(mapping.get_index(), mapping.get_type(), hit.getId());
                                updateRequest.doc(esFieldData);
                                bulkRequest.add(updateRequest);
                            }
                        }

                        if (bulkRequest.numberOfActions() % mapping.getCommitBatch() == 0
                            && bulkRequest.numberOfActions() > 0) {
                            long esBatchBegin = System.currentTimeMillis();
                            BulkResponse rp = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                            if (rp.hasFailures()) {
                                this.processFailBulkResponse(rp);
                            }

                            if (logger.isTraceEnabled()) {
                                logger.trace("全量数据批量导入批次耗时: {}, es执行时间: {}, 批次大小: {}, index; {}",
                                    (System.currentTimeMillis() - batchBegin),
                                    (System.currentTimeMillis() - esBatchBegin),
                                    bulkRequest.numberOfActions(),
                                    mapping.get_index());
                            }
                            batchBegin = System.currentTimeMillis();
                            bulkRequest = new BulkRequest();
                        }
                        count++;
                        impCount.incrementAndGet();
                    }

                    if (bulkRequest.numberOfActions() > 0) {
                        long esBatchBegin = System.currentTimeMillis();
                        BulkResponse rp = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                        if (rp.hasFailures()) {
                            this.processFailBulkResponse(rp);
                        }
                        if (logger.isTraceEnabled()) {
                            logger.trace("全量数据批量导入最后批次耗时: {}, es执行时间: {}, 批次大小: {}, index; {}",
                                (System.currentTimeMillis() - batchBegin),
                                (System.currentTimeMillis() - esBatchBegin),
                                bulkRequest.numberOfActions(),
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
