/**
 * Created by Wu Jian Ping on - 2021/09/15.
 */

package com.alibaba.otter.canal.client.adapter.http.service;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import javax.sql.DataSource;
import java.sql.*;
import com.google.common.base.Joiner;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.http.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.http.config.MappingConfig.EtlSetting;
import com.alibaba.otter.canal.client.adapter.http.config.MappingConfig.HttpMapping;
import com.alibaba.otter.canal.client.adapter.http.support.HttpTemplate;
import com.alibaba.otter.canal.client.adapter.support.AbstractEtlService;
import com.alibaba.otter.canal.client.adapter.support.AdapterConfig;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.Util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class HttpEtlService extends AbstractEtlService {

    private HttpTemplate httpTemplate;
    private MappingConfig config;

    public HttpEtlService(HttpTemplate httpTemplate, MappingConfig config) {
        super("http", config);
        this.httpTemplate = httpTemplate;
        this.config = config;
    }

    public EtlResult importData(List<String> params) {
        EtlResult etlResult = new EtlResult();
        AtomicLong impCount = new AtomicLong();
        List<String> errMsg = new ArrayList<>();
        HttpMapping httpMapping = this.config.getHttpMapping();
        EtlSetting etlSetting = httpMapping.getEtlSetting();

        String sql = "select * from " + etlSetting.getDatabase() + "." + etlSetting.getTable();

        long start = System.currentTimeMillis();
        try {
            DruidDataSource dataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());

            String etlCondition = null;

            List<Object> values = new ArrayList<>();
            // 拼接条件
            if (etlSetting.getCondition() != null && params != null) {
                etlCondition = etlSetting.getCondition();
                for (String param : params) {
                    etlCondition = etlCondition.replace("{}", "?");
                    values.add(param);
                }

                sql += " " + etlCondition;
            }

            if (logger.isDebugEnabled()) {
                logger.debug("etl sql : {}, values:{}", sql, values);
            }

            // 获取总数，这边直接通过主表获取，假如存在条件的话，拼接一下条件
            String countSql = "SELECT COUNT(1) FROM " + etlSetting.getDatabase() + "." + etlSetting.getTable();

            if (etlCondition != null) {
                countSql += " " + etlCondition;
            }

            if (logger.isInfoEnabled()) {
                logger.info("HTTP 全量count sql : {}, values:{}", countSql, values);
            }

            long total = (Long) Util.sqlRS(dataSource, countSql, values, rs -> {
                Long count = null;
                try {
                    if (rs.next()) {
                        count = ((Number) rs.getObject(1)).longValue();
                        if (logger.isInfoEnabled()) {
                            logger.info("HTTP 待同步数据总量:{}", count);
                        }
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
                return count == null ? 0L : count;
            });
            Boolean result = executeSqlImport(dataSource, sql, values, httpMapping, impCount, errMsg);
            if (result) {
                String s = String.format("HTTP 数据全量导入完成, 总共: %s 条数据，导入成功: %s 条数据, 耗时: %s 毫秒", total, impCount.get(),
                        System.currentTimeMillis() - start);
                logger.info(s);
                etlResult.setResultMessage(s);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            errMsg.add("HTTP 数据导入异常 =>" + e.getMessage());
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

        EtlSetting etlSetting = ((HttpMapping) adapterMapping).getEtlSetting();

        if (logger.isInfoEnabled()) {
            logger.info("HTTP 批量导入, threads: {}, batchSize: {}, sql:{}, values:{}", etlSetting.getThreads(),
                    etlSetting.getBatchSize(), sql, values);
        }

        List<String> columns = new ArrayList<>();

        try {
            ExecutorService executor = Util.newFixedThreadPool(etlSetting.getThreads(), 5000L);
            List<Future<Boolean>> futures = new ArrayList<>();
            Util.sqlRS(ds, sql, values, rs -> {
                try {
                    final List<Map<String, Object>> cachedDmls = new ArrayList<>();
                    while (rs.next()) {
                        // 理论上不搞锁也没关系
                        if (columns.size() == 0) {
                            synchronized (this.getClass()) {
                                if (columns.size() == 0) {
                                    ResultSetMetaData metaData = rs.getMetaData();
                                    int columnCount = metaData.getColumnCount();
                                    for (int i = 1; i <= columnCount; ++i) {
                                        columns.add(metaData.getColumnName(i));
                                    }
                                }
                            }
                        }

                        Map<String, Object> data = new LinkedHashMap<>();
                        for (String col : columns) {
                            data.put(col, rs.getObject(col));
                        }

                        Map<String, Object> item = new LinkedHashMap<>();
                        item.put("database", etlSetting.getDatabase());
                        item.put("table", etlSetting.getTable());
                        item.put("action", "update");
                        item.put("data", data);

                        cachedDmls.add(item);

                        if (cachedDmls.size() >= etlSetting.getBatchSize()) {
                            List<Map<String, Object>> tempCachedDmls = new ArrayList<>();
                            for (Map<String, Object> o : cachedDmls) {
                                tempCachedDmls.add(o);
                            }

                            cachedDmls.clear();

                            Future<Boolean> future = executor
                                    .submit(() -> this.httpTemplate.execute("etl", tempCachedDmls, impCount));

                            futures.add(future);
                        }
                    }

                    if (cachedDmls.size() > 0) {
                        List<Map<String, Object>> tempCachedDmls = new ArrayList<>();
                        for (Map<String, Object> o : cachedDmls) {
                            tempCachedDmls.add(o);
                        }

                        Future<Boolean> future = executor
                                .submit(() -> this.httpTemplate.execute("etl", tempCachedDmls, impCount));

                        futures.add(future);
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    errMsg.add("HTTP etl failed! ==>" + e.getMessage());
                    throw new RuntimeException(e);
                }
                return 0;
            });

            for (Future<Boolean> future : futures) {
                future.get();
            }
            executor.shutdown();
            return true;
        } catch (Exception e) {
            errMsg.add("HTTP 数据导入异常 => " + e.getMessage());
            return false;
        }
    }
}
