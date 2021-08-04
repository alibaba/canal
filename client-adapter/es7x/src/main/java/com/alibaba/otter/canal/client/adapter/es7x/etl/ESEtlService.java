package com.alibaba.otter.canal.client.adapter.es7x.etl;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.alibaba.otter.canal.client.adapter.es.core.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.core.config.ESSyncConfig.ESMapping;
import com.alibaba.otter.canal.client.adapter.es.core.config.ESSyncConfig.JoinSetting;
import com.alibaba.otter.canal.client.adapter.es.core.config.SchemaItem.FieldItem;
import com.alibaba.otter.canal.client.adapter.es.core.support.ESBulkRequest;
import com.alibaba.otter.canal.client.adapter.es.core.support.ESBulkRequest.ESBulkResponse;
import com.alibaba.otter.canal.client.adapter.es.core.support.ESBulkRequest.ESIndexRequest;
import com.alibaba.otter.canal.client.adapter.es.core.support.ESBulkRequest.ESUpdateRequest;
import com.alibaba.otter.canal.client.adapter.es.core.support.ESTemplate;
import com.alibaba.otter.canal.client.adapter.es7x.support.ES7xTemplate;
import com.alibaba.otter.canal.client.adapter.es7x.support.ESConnection;
import com.alibaba.otter.canal.client.adapter.es7x.support.ESConnection.ESSearchRequest;
import com.alibaba.otter.canal.client.adapter.support.AbstractEtlService;
import com.alibaba.otter.canal.client.adapter.support.AdapterConfig;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.Util;

import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import java.util.concurrent.ExecutorService;
import java.util.ArrayList;
import com.alibaba.druid.pool.DruidDataSource;
import java.util.concurrent.Future;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ES ETL Service
 *
 * @author rewerma 2018-11-01
 * @version 1.0.0
 */
public class ESEtlService extends AbstractEtlService {

    private static final Logger logger = LoggerFactory.getLogger(AbstractEtlService.class);

    private ESConnection esConnection;
    private ESTemplate esTemplate;
    private ESSyncConfig config;
    private final long CNT_PER_TASK = 10000L;
    private final String type = "ES";

    public ESEtlService(ESConnection esConnection, ESSyncConfig config) {
        super("ES", config);
        this.esConnection = esConnection;
        this.esTemplate = new ES7xTemplate(esConnection);
        this.config = config;
    }

    public EtlResult importData(List<String> params) {
        ESMapping mapping = config.getEsMapping();
        logger.info("start etl to import data to index: {}", mapping.get_index());
        String sql = mapping.getSql();
        logger.info("sql:{}", sql);
        return importData(sql, params);
    }

    // Added by Wu Jian Ping, Instread of AbstractEtlService.importData
    protected EtlResult importData(String sql, List<String> params) {
        EtlResult etlResult = new EtlResult();
        AtomicLong impCount = new AtomicLong();
        List<String> errMsg = new ArrayList<>();
        if (config == null) {
            logger.warn("{} mapping config is null, etl go end ", type);
            etlResult.setErrorMessage(type + "mapping config is null, etl go end ");
            return etlResult;
        }

        long start = System.currentTimeMillis();
        try {
            DruidDataSource dataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());

            String etlCondition = null;

            List<Object> values = new ArrayList<>();
            // 拼接条件
            if (config.getMapping().getEtlCondition() != null && params != null) {
                etlCondition = config.getMapping().getEtlCondition();
                for (String param : params) {
                    etlCondition = etlCondition.replace("{}", "?");
                    values.add(param);
                }

                sql += " " + etlCondition;
            }

            if (logger.isDebugEnabled()) {
                logger.debug("etl sql : {}, values:{}", sql, values);
            }

            JoinSetting joinSetting = null;
            for (JoinSetting setting : config.getEsMapping().getJoinSettings().values()) {
                if (setting.getPrimary()) {
                    joinSetting = setting;
                    break;
                }
            }

            if (joinSetting == null) {
                logger.error("Cannot find primry table defination in joinSettings");
                throw new RuntimeException("Cannot find primry table defination in joinSettings");
            }

            // 获取总数，这边直接通过主表获取，假如存在条件的话，拼接一下条件
            String countSql = "SELECT COUNT(1) FROM " + joinSetting.getTableName() + " " + joinSetting.getAliasName();

            if (etlCondition != null) {
                countSql += " " + etlCondition;
            }

            if (logger.isInfoEnabled()) {
                logger.info("ES7 全量count sql : {}, values:{}", countSql, values);
            }

            long cnt = (Long) Util.sqlRS(dataSource, countSql, values, rs -> {
                Long count = null;
                try {
                    if (rs.next()) {
                        count = ((Number) rs.getObject(1)).longValue();
                        if (logger.isInfoEnabled()) {
                            logger.info("ES7 待同步数据总量:{}", count);
                        }
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
                return count == null ? 0L : count;
            });

            // 当大于1万条记录时开启多线程
            // NOTE: 采用limit分页取数据的方式，大表性能存在问题，使用线程并不能解决该问题，实际上线程在这里的价值并不大
            if (cnt >= 10000) {
                int threadCount = Runtime.getRuntime().availableProcessors();

                long offset;
                long size = CNT_PER_TASK;
                long workerCnt = cnt / size + (cnt % size == 0 ? 0 : 1);

                if (logger.isDebugEnabled()) {
                    logger.debug("workerCnt {} for cnt {} threadCount {}", workerCnt, cnt, threadCount);
                }

                // 假如不存在etlCondition，且存在sequenceColumnName的配置
                if (joinSetting.getSequenceColumnName() != null && etlCondition == null) {
                    // 获取一下最大值
                    String maxSequenceSql = "SELECT MAX(" + joinSetting.getSequenceColumnName() + ") FROM "
                            + joinSetting.getTableName();
                    long maxSequence = (Long) Util.sqlRS(dataSource, maxSequenceSql, values, rs -> {
                        Long max = null;
                        try {
                            if (rs.next()) {
                                max = ((Number) rs.getObject(1)).longValue();
                            }
                        } catch (Exception e) {
                            logger.error(e.getMessage(), e);
                        }
                        return max == null ? 0L : max;
                    });

                    ExecutorService executor = Util.newFixedThreadPool(threadCount, 5000L);
                    List<Future<Boolean>> futures = new ArrayList<>();
                    for (long i = 0; i < workerCnt; i++) {
                        long startSequence = i * new Double(Math.floor(maxSequence * 1.0 / workerCnt)).longValue();
                        long endSequence = (i + 1) * new Double(Math.floor(maxSequence * 1.0 / workerCnt)).longValue();
                        // 生成栏位名，如：a.id,
                        // 最终生成SQL为：
                        // select xxx from user a where a.id > 5000000 and a.id < 5010000 limit 1000
                        String sequenceColumnRealName = joinSetting.getAliasName() + "."
                                + joinSetting.getSequenceColumnName();
                        String sqlFinal = sql + " WHERE " + sequenceColumnRealName + " > " + startSequence + " AND "
                                + sequenceColumnRealName + " <= " + endSequence + " LIMIT "
                                + (endSequence - startSequence);

                        if (logger.isInfoEnabled()) {
                            logger.info("ES7 全量分批导入, sql: {}", sqlFinal);
                        }

                        Future<Boolean> future = executor.submit(() -> executeSqlImport(dataSource, sqlFinal, values,
                                config.getMapping(), impCount, errMsg));
                        futures.add(future);
                    }

                    for (Future<Boolean> future : futures) {
                        future.get();
                    }
                    executor.shutdown();

                } else {
                    ExecutorService executor = Util.newFixedThreadPool(threadCount, 5000L);
                    List<Future<Boolean>> futures = new ArrayList<>();
                    for (long i = 0; i < workerCnt; i++) {
                        offset = size * i;
                        String sqlFinal = sql + " LIMIT " + offset + "," + size;

                        if (logger.isInfoEnabled()) {
                            logger.info("ES7 全量分批导入, sql: {}, values:{}", sqlFinal, values);
                        }

                        Future<Boolean> future = executor.submit(() -> executeSqlImport(dataSource, sqlFinal, values,
                                config.getMapping(), impCount, errMsg));
                        futures.add(future);
                    }

                    for (Future<Boolean> future : futures) {
                        future.get();
                    }
                    executor.shutdown();
                }
            } else {
                executeSqlImport(dataSource, sql, values, config.getMapping(), impCount, errMsg);
            }

            logger.info("ES7 数据全量导入完成, 一共导入 {} 条数据, 耗时: {}", impCount.get(), System.currentTimeMillis() - start);
            etlResult.setResultMessage("导入" + type + " 数据：" + impCount.get() + " 条");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            errMsg.add(type + " 数据导入异常 =>" + e.getMessage());
        }
        if (errMsg.isEmpty()) {
            etlResult.setSucceeded(true);
        } else {
            etlResult.setErrorMessage(Joiner.on("\n").join(errMsg));
        }
        return etlResult;
    }

    protected boolean executeSqlImport(DataSource ds, String sql, List<Object> values,
            AdapterConfig.AdapterMapping adapterMapping, AtomicLong impCount, List<String> errMsg) {
        try {
            ESMapping mapping = (ESMapping) adapterMapping;

            if (logger.isDebugEnabled()) {
                logger.debug("ES7批量导入, sql:{} values:{}", sql, values);
            }

            Util.sqlRS(ds, sql, values, rs -> {
                int count = 0;
                try {
                    ESBulkRequest esBulkRequest = this.esConnection.new ES7xBulkRequest();

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
                                    FieldItem parentFieldItem = mapping.getSchemaItem().getSelectFields()
                                            .get(relationMapping.getParent());
                                    Object parentVal;
                                    try {
                                        parentVal = esTemplate.getValFromRS(mapping, rs, parentFieldItem.getFieldName(),
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
                                ESUpdateRequest esUpdateRequest = this.esConnection.new ES7xUpdateRequest(
                                        mapping.get_index(), idVal.toString()).setDoc(esFieldData).setDocAsUpsert(true);

                                if (StringUtils.isNotEmpty(parentVal)) {
                                    esUpdateRequest.setRouting(parentVal);
                                }

                                esBulkRequest.add(esUpdateRequest);
                            } else {
                                ESIndexRequest esIndexRequest = this.esConnection.new ES7xIndexRequest(
                                        mapping.get_index(), idVal.toString()).setSource(esFieldData);
                                if (StringUtils.isNotEmpty(parentVal)) {
                                    esIndexRequest.setRouting(parentVal);
                                }
                                esBulkRequest.add(esIndexRequest);
                            }
                        } else {
                            idVal = esFieldData.get(mapping.getPk());
                            ESSearchRequest esSearchRequest = this.esConnection.new ESSearchRequest(mapping.get_index())
                                    .setQuery(QueryBuilders.termQuery(mapping.getPk(), idVal)).size(10000);
                            SearchResponse response = esSearchRequest.getResponse();
                            for (SearchHit hit : response.getHits()) {
                                ESUpdateRequest esUpdateRequest = this.esConnection.new ES7xUpdateRequest(
                                        mapping.get_index(), hit.getId()).setDoc(esFieldData);
                                esBulkRequest.add(esUpdateRequest);
                            }
                        }

                        if (esBulkRequest.numberOfActions() % mapping.getCommitBatch() == 0
                                && esBulkRequest.numberOfActions() > 0) {
                            long esBatchBegin = System.currentTimeMillis();
                            ESBulkResponse rp = esBulkRequest.bulk();
                            if (rp.hasFailures()) {
                                rp.processFailBulkResponse("全量数据 etl 异常 ");
                            }

                            if (logger.isTraceEnabled()) {
                                logger.trace("全量数据批量导入批次耗时: {}, es执行时间: {}, 批次大小: {}, index; {}",
                                        (System.currentTimeMillis() - batchBegin),
                                        (System.currentTimeMillis() - esBatchBegin), esBulkRequest.numberOfActions(),
                                        mapping.get_index());
                            }
                            batchBegin = System.currentTimeMillis();
                            esBulkRequest.resetBulk();
                        }
                        count++;
                        impCount.incrementAndGet();
                    }

                    if (esBulkRequest.numberOfActions() > 0) {
                        long esBatchBegin = System.currentTimeMillis();
                        ESBulkResponse rp = esBulkRequest.bulk();
                        if (rp.hasFailures()) {
                            rp.processFailBulkResponse("全量数据 etl 异常 ");
                        }
                        if (logger.isTraceEnabled()) {
                            logger.trace("全量数据批量导入最后批次耗时: {}, es执行时间: {}, 批次大小: {}, index; {}",
                                    (System.currentTimeMillis() - batchBegin),
                                    (System.currentTimeMillis() - esBatchBegin), esBulkRequest.numberOfActions(),
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
