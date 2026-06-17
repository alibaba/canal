package com.alibaba.otter.canal.client.adapter.support;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
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

    private long getPage(long id, long pageSize) {
        return id / pageSize;
    }

    private long getCnt(DataSource dataSource, String sql, List<Object> values) {
        return (Long) Util.sqlRS(dataSource, sql, values, rs -> {
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
    }

    /**
     * 如果etl没有条件，全量进行elt，使用主键ID进行遍历
     * @param sql  origin sql
     * @param primaryKey  pk in table
     * @param simpleTable  one table name
     * @return etlResult  return result
     */
    private EtlResult importDataByPKWithoutCondition(String sql, String primaryKey, String simpleTable) {
        logger.info("primary key {} simpleTable {}", primaryKey, simpleTable);
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

            List<Object> values = new ArrayList<>();

            if (logger.isDebugEnabled()) {
                logger.debug("etl sql : {}", sql);
            }

            String maxKeySql = "SELECT max("+primaryKey+") from "+ simpleTable;
            long maxId = getCnt(dataSource, maxKeySql, values);
            long endPage = getPage(maxId, CNT_PER_TASK);

            String minKeySql = "SELECT min("+primaryKey+") from "+ simpleTable;
            long minId = getCnt(dataSource, minKeySql, values);
            long startPage = getPage(minId, CNT_PER_TASK);

            logger.info("maxId {} minId {}", maxId, minId);

            // 当大于1万条记录时开启多线程
            if (maxId - minId >= 10000) {
                int threadCount = Runtime.getRuntime().availableProcessors();

                long workerCnt = endPage - startPage + 1;
                long startId;
                long endId;

                if (logger.isDebugEnabled()) {
                    logger.debug("workerCnt {} for threadCount {}", workerCnt, threadCount);
                }

                ExecutorService executor = Util.newFixedThreadPool(threadCount, 5000L);
                List<Future<Boolean>> futures = new ArrayList<>();
                for (long i = 0; i < workerCnt; i++) {
                    startId = CNT_PER_TASK * i;
                    endId = startId + CNT_PER_TASK;
                    String sqlFinal = sql + " where  " + primaryKey + " between " + startId + " and " + endId;
                    Future<Boolean> future = executor.submit(() -> executeSqlImport(dataSource,
                            sqlFinal,
                            values,
                            config.getMapping(),
                            impCount,
                            errMsg));
                    futures.add(future);
                }

                for (Future<Boolean> future : futures) {
                    future.get();
                }
                executor.shutdown();
            } else {
                executeSqlImport(dataSource, sql, values, config.getMapping(), impCount, errMsg);
            }

            logger.info("数据全量导入完成, 一共导入 {} 条数据, 耗时: {}", impCount.get(), System.currentTimeMillis() - start);
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


    protected EtlResult importData(String sql, List<String> params) {
        EtlResult etlResult = new EtlResult();

        if (config == null) {
            logger.warn("{} mapping config is null, etl go end ", type);
            etlResult.setErrorMessage(type + "mapping config is null, etl go end ");
            return etlResult;
        }

        logger.info("etl condition {}, is simple table {}", config.getMapping().getEtlCondition(), config.getMapping().isSimpleTable());
        if (StringUtils.isEmpty(config.getMapping().getEtlCondition()) && config.getMapping().isSimpleTable()) {
            return importDataByPKWithoutCondition(sql, config.getMapping().getPrimaryKey(), config.getMapping().getTable());
        }

        return importDataWithCondition(sql, params);
    }

    protected EtlResult importDataWithCondition(String sql, List<String> params) {
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

            List<Object> values = new ArrayList<>();
            // 拼接条件
            if (config.getMapping().getEtlCondition() != null && params != null) {
                String etlCondition = config.getMapping().getEtlCondition();
                for (String param : params) {
                    etlCondition = etlCondition.replace("{}", "?");
                    values.add(param);
                }

                sql += " " + etlCondition;
            }

            if (logger.isDebugEnabled()) {
                logger.debug("etl sql : {}", sql);
            }

            // 获取总数
            String countSql = "SELECT COUNT(1) FROM ( " + sql + ") _CNT ";
            long cnt = (Long) Util.sqlRS(dataSource, countSql, values, rs -> {
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
                int threadCount = Runtime.getRuntime().availableProcessors();

                long offset;
                long size = CNT_PER_TASK;
                long workerCnt = cnt / size + (cnt % size == 0 ? 0 : 1);

                if (logger.isDebugEnabled()) {
                    logger.debug("workerCnt {} for cnt {} threadCount {}", workerCnt, cnt, threadCount);
                }

                ExecutorService executor = Util.newFixedThreadPool(threadCount, 5000L);
                List<Future<Boolean>> futures = new ArrayList<>();
                for (long i = 0; i < workerCnt; i++) {
                    offset = size * i;
                    String sqlFinal = sql + " LIMIT " + offset + "," + size;
                    Future<Boolean> future = executor.submit(() -> executeSqlImport(dataSource,
                        sqlFinal,
                        values,
                        config.getMapping(),
                        impCount,
                        errMsg));
                    futures.add(future);
                }

                for (Future<Boolean> future : futures) {
                    future.get();
                }
                executor.shutdown();
            } else {
                executeSqlImport(dataSource, sql, values, config.getMapping(), impCount, errMsg);
            }

            logger.info("数据全量导入完成, 一共导入 {} 条数据, 耗时: {}", impCount.get(), System.currentTimeMillis() - start);
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
                                                AdapterConfig.AdapterMapping mapping, AtomicLong impCount,
                                                List<String> errMsg);

}
