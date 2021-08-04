package com.alibaba.otter.canal.client.adapter.support;

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

    protected EtlResult importData(String sql, List<String> params, String tableFullName, String sequenceColumnName) {
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

                // 假如不存在etlCondition，且存在sequenceColumnName的配置
                if (etlCondition == null && sequenceColumnName != null) {
                    // 获取一下最大值
                    String maxSequenceSql = "SELECT MAX(" + sequenceColumnName + ") FROM " + tableFullName;
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
                        // 最终生成SQL为：
                        // <raw sql> where sequence > 5000000 and sequence < 5010000 limit 1000
                        String sqlFinal = sql + " WHERE " + sequenceColumnName + " > " + startSequence + " AND "
                                + sequenceColumnName + " <= " + endSequence + " LIMIT " + size;

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
