package com.alibaba.otter.canal.client.adapter.support;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.pool.DruidDataSource;
import com.google.common.base.Joiner;

public abstract class AbstractEtlService {

    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    private String type;
    private AdapterConfig config;
    private final long CNT_PER_TASK = 10000L;

    public AbstractEtlService(String type, AdapterConfig config) {
        this.type = type;
        this.config = config;
    }

    protected EtlResult importData(String sql, List<String> params, String tableFullName, String sequenceColumn) {
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

            // 获取总数，这边直接通过主表获取，假如存在条件的话，拼接一下条件
            String countSql = "SELECT COUNT(1) FROM " + tableFullName;

            if (etlCondition != null) {
                countSql += " " + etlCondition;
            }

            if (logger.isInfoEnabled()) {
                logger.info(type + " 全量count sql : {}, values:{}", countSql, values);
            }

            long cnt = (Long) Util.sqlRS(dataSource, countSql, values, rs -> {
                Long count = null;
                try {
                    if (rs.next()) {
                        count = ((Number) rs.getObject(1)).longValue();
                        if (logger.isInfoEnabled()) {
                            logger.info(type + " 待同步数据总量:{}", count);
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

                // 假如存在sequenceColumn的配置
                if (sequenceColumn != null) {
                    // 获取一下最大值
                    String sequenceSql = "SELECT MIN(" + sequenceColumn + ") as min, MAX(" + sequenceColumn
                            + ") as max FROM " + tableFullName;

                    if (etlCondition != null) {
                        sequenceSql += " " + etlCondition;
                    }

                    @SuppressWarnings("unchecked")
                    HashMap<String, Long> sequenceMap = (HashMap<String, Long>) Util.sqlRS(dataSource, sequenceSql,
                            values, rs -> {
                                Long max = null;
                                Long min = null;
                                try {
                                    if (rs.next()) {
                                        min = ((Number) rs.getObject("min")).longValue();
                                        max = ((Number) rs.getObject("max")).longValue();
                                    }
                                } catch (Exception e) {
                                    logger.error(e.getMessage(), e);
                                }
                                max = (max == null ? 1L : max);
                                min = (min == null ? 1L : min);

                                HashMap<String, Long> map = new HashMap<>();
                                map.put("min", min);
                                map.put("max", max);

                                return map;
                            });

                    long maxSequence = sequenceMap.get("max");
                    long minSequence = sequenceMap.get("min") - 1;

                    if (logger.isInfoEnabled()) {
                        logger.info("ES7 全量导入, start:{}, end:{}", minSequence, maxSequence);
                    }

                    ExecutorService executor = Util.newFixedThreadPool(threadCount, 5000L);
                    List<Future<Boolean>> futures = new ArrayList<>();
                    for (long i = 0; i < workerCnt; i++) {
                        long startSequence = minSequence
                                + i * new Double(Math.floor(maxSequence * 1.0 / workerCnt)).longValue();
                        long endSequence = minSequence
                                + (i + 1) * new Double(Math.floor(maxSequence * 1.0 / workerCnt)).longValue();
                        // 最终生成SQL为：
                        // <raw sql> where sequence > 5000000 and sequence < 5010000 limit 1000
                        String sqlFinal = sql + (etlCondition == null ? " WHERE " : " AND ") + sequenceColumn + " > "
                                + startSequence + " AND " + sequenceColumn + " <= " + endSequence + " LIMIT "
                                + (endSequence - startSequence);

                        if (logger.isDebugEnabled()) {
                            logger.debug(type + " 全量分批导入, sql: {}", sqlFinal);
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

                        if (logger.isDebugEnabled()) {
                            logger.debug(type + " 全量分批导入, sql: {}, values:{}", sqlFinal, values);
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

            logger.info(type + " 数据全量导入完成, 一共导入 {} 条数据, 耗时: {}", impCount.get(), System.currentTimeMillis() - start);
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

    protected abstract boolean executeSqlImport(DataSource ds, String sql, List<Object> values,
            AdapterConfig.AdapterMapping mapping, AtomicLong impCount, List<String> errMsg);

}
