package com.alibaba.otter.canal.client.adapter.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.hbase.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.hbase.config.MappingConfigLoader;
import com.alibaba.otter.canal.client.adapter.hbase.monitor.HbaseConfigMonitor;
import com.alibaba.otter.canal.client.adapter.hbase.service.HbaseEtlService;
import com.alibaba.otter.canal.client.adapter.hbase.service.HbaseSyncService;
import com.alibaba.otter.canal.client.adapter.hbase.support.HbaseTemplate;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;
import com.alibaba.otter.canal.client.adapter.support.SPI;

/**
 * HBase外部适配器
 *
 * @author machengyuan 2018-8-21 下午8:45:38
 * @version 1.0.0
 */
@SPI("hbase")
public class HbaseAdapter implements OuterAdapter {

    private static Logger                           logger             = LoggerFactory.getLogger(HbaseAdapter.class);

    private Map<String, MappingConfig>              hbaseMapping       = new ConcurrentHashMap<>();                  // 文件名对应配置
    private Map<String, Map<String, MappingConfig>> mappingConfigCache = new ConcurrentHashMap<>();                  // 库名-表名对应配置

    private Connection                              conn;
    private HbaseSyncService                        hbaseSyncService;
    private HbaseTemplate                           hbaseTemplate;

    private HbaseConfigMonitor                      configMonitor;

    public Map<String, MappingConfig> getHbaseMapping() {
        return hbaseMapping;
    }

    public Map<String, Map<String, MappingConfig>> getMappingConfigCache() {
        return mappingConfigCache;
    }

    @Override
    public void init(OuterAdapterConfig configuration) {
        try {
            Map<String, MappingConfig> hbaseMappingTmp = MappingConfigLoader.load();
            // 过滤不匹配的key的配置
            hbaseMappingTmp.forEach((key, mappingConfig) -> {
                if ((mappingConfig.getOuterAdapterKey() == null && configuration.getKey() == null)
                    || (mappingConfig.getOuterAdapterKey() != null
                        && mappingConfig.getOuterAdapterKey().equalsIgnoreCase(configuration.getKey()))) {
                    hbaseMapping.put(key, mappingConfig);
                }
            });
            for (Map.Entry<String, MappingConfig> entry : hbaseMapping.entrySet()) {
                String configName = entry.getKey();
                MappingConfig mappingConfig = entry.getValue();
                String k = StringUtils.trimToEmpty(mappingConfig.getDestination()) + "."
                           + mappingConfig.getHbaseMapping().getDatabase() + "."
                           + mappingConfig.getHbaseMapping().getTable();
                Map<String, MappingConfig> configMap = mappingConfigCache.computeIfAbsent(k, k1 -> new HashMap<>());
                configMap.put(configName, mappingConfig);
            }

            Map<String, String> properties = configuration.getProperties();

            Configuration hbaseConfig = HBaseConfiguration.create();
            properties.forEach(hbaseConfig::set);
            conn = ConnectionFactory.createConnection(hbaseConfig);
            hbaseTemplate = new HbaseTemplate(conn);
            hbaseSyncService = new HbaseSyncService(hbaseTemplate);

            configMonitor = new HbaseConfigMonitor();
            configMonitor.init(this);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void sync(List<Dml> dmls) {
        for (Dml dml : dmls) {
            sync(dml);
        }
    }

    public void sync(Dml dml) {
        if (dml == null) {
            return;
        }
        String destination = StringUtils.trimToEmpty(dml.getDestination());
        String database = dml.getDatabase();
        String table = dml.getTable();
        Map<String, MappingConfig> configMap = mappingConfigCache.get(destination + "." + database + "." + table);
        configMap.values().forEach(config -> hbaseSyncService.sync(config, dml));
    }

    @Override
    public EtlResult etl(String task, List<String> params) {
        EtlResult etlResult = new EtlResult();
        MappingConfig config = hbaseMapping.get(task);
        if (config != null) {
            DataSource dataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
            if (dataSource != null) {
                return HbaseEtlService.importData(dataSource, hbaseTemplate, config, params);
            } else {
                etlResult.setSucceeded(false);
                etlResult.setErrorMessage("DataSource not found");
                return etlResult;
            }
        } else {
            StringBuilder resultMsg = new StringBuilder();
            boolean resSucc = true;
            // ds不为空说明传入的是datasourceKey
            for (MappingConfig configTmp : hbaseMapping.values()) {
                // 取所有的destination为task的配置
                if (configTmp.getDestination().equals(task)) {
                    DataSource dataSource = DatasourceConfig.DATA_SOURCES.get(configTmp.getDataSourceKey());
                    if (dataSource == null) {
                        continue;
                    }
                    EtlResult etlRes = HbaseEtlService.importData(dataSource, hbaseTemplate, configTmp, params);
                    if (!etlRes.getSucceeded()) {
                        resSucc = false;
                        resultMsg.append(etlRes.getErrorMessage()).append("\n");
                    } else {
                        resultMsg.append(etlRes.getResultMessage()).append("\n");
                    }
                }
            }
            if (resultMsg.length() > 0) {
                etlResult.setSucceeded(resSucc);
                if (resSucc) {
                    etlResult.setResultMessage(resultMsg.toString());
                } else {
                    etlResult.setErrorMessage(resultMsg.toString());
                }
                return etlResult;
            }
        }
        etlResult.setSucceeded(false);
        etlResult.setErrorMessage("Task not found");
        return etlResult;
    }

    @Override
    public Map<String, Object> count(String task) {
        MappingConfig config = hbaseMapping.get(task);
        String hbaseTable = config.getHbaseMapping().getHbaseTable();
        long rowCount = 0L;
        try {
            HTable table = (HTable) conn.getTable(TableName.valueOf(hbaseTable));
            Scan scan = new Scan();
            scan.setFilter(new FirstKeyOnlyFilter());
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                rowCount += result.size();
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        Map<String, Object> res = new LinkedHashMap<>();
        res.put("hbaseTable", hbaseTable);
        res.put("count", rowCount);
        return res;
    }

    @Override
    public void destroy() {
        if (configMonitor != null) {
            configMonitor.destroy();
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public String getDestination(String task) {
        MappingConfig config = hbaseMapping.get(task);
        if (config != null && config.getHbaseMapping() != null) {
            return config.getDestination();
        }
        return null;
    }
}
