package com.alibaba.otter.canal.client.adapter.kudu.service;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.sql.DataSource;

import org.apache.kudu.client.KuduException;

import com.alibaba.otter.canal.client.adapter.kudu.config.KuduMappingConfig;
import com.alibaba.otter.canal.client.adapter.kudu.support.KuduTemplate;
import com.alibaba.otter.canal.client.adapter.kudu.support.SyncUtil;
import com.alibaba.otter.canal.client.adapter.support.AbstractEtlService;
import com.alibaba.otter.canal.client.adapter.support.AdapterConfig;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.Util;
import com.google.common.base.Joiner;

/**
 * @author liuyadong
 * @description kudu 拉取历史数据
 */
public class KuduEtlService extends AbstractEtlService {

    private KuduTemplate      kuduTemplate;
    private KuduMappingConfig config;

    public KuduEtlService(KuduTemplate kuduTemplate, KuduMappingConfig config){
        super("kudu", config);
        this.kuduTemplate = kuduTemplate;
        this.config = config;
    }

    public EtlResult importData(List<String> params) {
        EtlResult etlResult = new EtlResult();
        List<String> errMsg = new ArrayList<>();

        KuduMappingConfig.KuduMapping kuduMapping = config.getKuduMapping();
        boolean flag = kuduTemplate.tableExists(kuduMapping.getTargetTable());
        // 表不存在，停止导入
        if (!flag) {
            logger.info("{} is don't hava,please check your kudu table !", kuduMapping.getTargetTable());
            errMsg.add(kuduMapping.getTargetTable() + " is don't hava,please check your kudu table !");
            etlResult.setErrorMessage(Joiner.on("\n").join(errMsg));
            return etlResult;
        }
        logger.info("{} etl is starting!", kuduMapping.getTargetTable());
        String sql = "SELECT * FROM " + kuduMapping.getDatabase() + "." + kuduMapping.getTable();
        return importData(sql, params);
    }

    @Override
    protected boolean executeSqlImport(DataSource ds, String sql, List<Object> values,
                                       AdapterConfig.AdapterMapping mapping, AtomicLong impCount, List<String> errMsg) {
        KuduMappingConfig.KuduMapping kuduMapping = (KuduMappingConfig.KuduMapping) mapping;
        // 获取字段元数据
        Map<String, String> columnsMap = new LinkedHashMap<>();// 需要同步的字段

        try {
            Util.sqlRS(ds, "SELECT * FROM " + SyncUtil.getDbTableName(kuduMapping) + " LIMIT 1", rs -> {
                try {
                    ResultSetMetaData rsd = rs.getMetaData();
                    int columnCount = rsd.getColumnCount();
                    List<String> columns = new ArrayList<>();
                    for (int i = 1; i <= columnCount; i++) {
                        columns.add(rsd.getColumnName(i).toLowerCase());
                    }
                    columnsMap.putAll(SyncUtil.getColumnsMap(kuduMapping, columns));
                    return true;
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    return false;
                }
            });
            // 写入数据
            logger.info("etl select data sql is :{}", sql);
            Util.sqlRS(ds, sql, values, rs -> {
                int idx = 1;
                try {
                    List<Map<String, Object>> dataList = new ArrayList<>();
                    while (rs.next()) {
                        Map<String, Object> data = new HashMap<>();
                        for (Map.Entry<String, String> entry : columnsMap.entrySet()) {
                            String mysqlColumnName = entry.getKey();// mysql字段名
                            String kuduColumnName = entry.getValue();// kudu字段名
                            if (kuduColumnName == null) {
                                kuduColumnName = mysqlColumnName;
                            }
                            Object value = rs.getObject(kuduColumnName);
                            if (value != null) {
                                data.put(kuduColumnName, value);
                            } else {
                                data.put(kuduColumnName, null);
                            }
                        }
                        dataList.add(data);
                        idx++;
                        impCount.incrementAndGet();
                        if (logger.isDebugEnabled()) {
                            logger.debug("successful import count:" + impCount.get());
                        }
                        if (idx % kuduMapping.getCommitBatch() == 0) {
                            kuduTemplate.upsert(kuduMapping.getTargetTable(), dataList);
                            dataList.clear();
                        }
                    }
                    if (!dataList.isEmpty()) {
                        kuduTemplate.upsert(kuduMapping.getTargetTable(), dataList);
                    }
                    return true;

                } catch (SQLException | KuduException e) {
                    e.printStackTrace();
                    logger.error(kuduMapping.getTargetTable() + " etl failed! ==>" + e.getMessage(), e);
                    errMsg.add(kuduMapping.getTargetTable() + " etl failed! ==>" + e.getMessage());
                    return false;
                }
            });
            return true;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        }

    }
}
