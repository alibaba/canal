package com.alibaba.otter.canal.client.adapter.hbase;

import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.hbase.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.hbase.config.MappingConfigLoader;
import com.alibaba.otter.canal.client.adapter.hbase.monitor.HbaseConfigMonitor;
import com.alibaba.otter.canal.client.adapter.hbase.service.HbaseEtlService;
import com.alibaba.otter.canal.client.adapter.hbase.service.HbaseSyncService;
import com.alibaba.otter.canal.client.adapter.hbase.support.HbaseTemplate;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.FileName2KeyMapping;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;
import com.alibaba.otter.canal.client.adapter.support.SPI;
import com.alibaba.otter.canal.client.adapter.support.Util;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private HbaseSyncService                        hbaseSyncService;
    private HbaseTemplate                           hbaseTemplate;

    private HbaseConfigMonitor                      configMonitor;

    private Properties                              envProperties;

    private OuterAdapterConfig                      configuration;

    public Map<String, MappingConfig> getHbaseMapping() {
        return hbaseMapping;
    }

    public Map<String, Map<String, MappingConfig>> getMappingConfigCache() {
        return mappingConfigCache;
    }

    @Override
    public void init(OuterAdapterConfig configuration, Properties envProperties) {
        try {
            this.envProperties = envProperties;
            this.configuration = configuration;
            Map<String, MappingConfig> hbaseMappingTmp = MappingConfigLoader.load(envProperties);
            // 过滤不匹配的key的配置
            hbaseMappingTmp.forEach((key, config) -> {
                addConfig(key, config);
            });

            Map<String, String> properties = configuration.getProperties();

            Configuration hbaseConfig = HBaseConfiguration.create();
            properties.forEach(hbaseConfig::set);
            hbaseTemplate = new HbaseTemplate(hbaseConfig);
            hbaseSyncService = new HbaseSyncService(hbaseTemplate);

            configMonitor = new HbaseConfigMonitor();
            configMonitor.init(this, envProperties);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void sync(List<Dml> dmls) {
        if (dmls == null || dmls.isEmpty()) {
            return;
        }
        for (Dml dml : dmls) {
            sync(dml);
        }
    }

    private void sync(Dml dml) {
        if (dml == null) {
            return;
        }
        String destination = StringUtils.trimToEmpty(dml.getDestination());
        String groupId = StringUtils.trimToEmpty(dml.getGroupId());
        String database = dml.getDatabase();
        String table = dml.getTable();
        Map<String, MappingConfig> configMap;
        if (envProperties != null && !"tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
            configMap = mappingConfigCache.get(destination + "-" + groupId + "_" + database + "-" + table);
        } else {
            configMap = mappingConfigCache.get(destination + "_" + database + "-" + table);
        }
        if (configMap != null) {
            List<MappingConfig> configs = new ArrayList<>();
            configMap.values().forEach(config -> {
                if (StringUtils.isNotEmpty(config.getGroupId())) {
                    if (config.getGroupId().equals(dml.getGroupId())) {
                        configs.add(config);
                    }
                } else {
                    configs.add(config);
                }
            });
            if (!configs.isEmpty()) {
                configs.forEach(config -> hbaseSyncService.sync(config, dml));
            }
        }
    }

    @Override
    public EtlResult etl(String task, List<String> params) {
        EtlResult etlResult = new EtlResult();
        MappingConfig config = hbaseMapping.get(task);
        HbaseEtlService hbaseEtlService = new HbaseEtlService(hbaseTemplate, config);
        if (config != null) {
            return hbaseEtlService.importData(params);
        } else {
            StringBuilder resultMsg = new StringBuilder();
            boolean resSucc = true;
            for (MappingConfig configTmp : hbaseMapping.values()) {
                // 取所有的destination为task的配置
                if (configTmp.getDestination().equals(task)) {
                    EtlResult etlRes = hbaseEtlService.importData(params);
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
            HTable table = (HTable) hbaseTemplate.getConnection().getTable(TableName.valueOf(hbaseTable));
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
        try {
            hbaseTemplate.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
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

    private void addSyncConfigToCache(String configName, MappingConfig mappingConfig) {
        String k;
        if (envProperties != null && !"tcp"
                .equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
            k = StringUtils.trimToEmpty(mappingConfig.getDestination()) + "-" + StringUtils
                    .trimToEmpty(mappingConfig.getGroupId()) + "_" + mappingConfig.getHbaseMapping()
                    .getDatabase() + "-" + mappingConfig.getHbaseMapping().getTable();
        } else {
            k = StringUtils.trimToEmpty(mappingConfig.getDestination()) + "_" + mappingConfig
                    .getHbaseMapping().getDatabase() + "-" + mappingConfig.getHbaseMapping()
                    .getTable();
        }
        Map<String, MappingConfig> configMap = mappingConfigCache
                .computeIfAbsent(k, k1 -> new ConcurrentHashMap<>());
        configMap.put(configName, mappingConfig);
    }

    public boolean addConfig(String fileName, MappingConfig config) {
        if (match(config)) {
            hbaseMapping.put(fileName, config);
            addSyncConfigToCache(fileName, config);
            FileName2KeyMapping.register(getClass().getAnnotation(SPI.class).value(), fileName,
                    configuration.getKey());
            return true;
        }
        return false;
    }

    public void updateConfig(String fileName, MappingConfig config) {
        if (config.getOuterAdapterKey() != null && !config.getOuterAdapterKey()
                .equals(configuration.getKey())) {
            // 理论上不允许改这个 因为本身就是通过这个关联起Adapter和Config的
            throw new RuntimeException("not allow to change outAdapterKey");
        }
        hbaseMapping.put(fileName, config);
        addSyncConfigToCache(fileName, config);
    }

    public void deleteConfig(String fileName) {
        hbaseMapping.remove(fileName);
        for (Map<String, MappingConfig> configMap : mappingConfigCache.values()) {
            if (configMap != null) {
                configMap.remove(fileName);
            }
        }
        FileName2KeyMapping.unregister(getClass().getAnnotation(SPI.class).value(), fileName);
    }

    private boolean match(MappingConfig config) {
        boolean sameMatch = config.getOuterAdapterKey() != null && config.getOuterAdapterKey()
                .equalsIgnoreCase(configuration.getKey());
        boolean prefixMatch = config.getOuterAdapterKey() == null && configuration.getKey()
                .startsWith(StringUtils
                        .join(new String[]{Util.AUTO_GENERATED_PREFIX, config.getDestination(),
                                config.getGroupId()}, '-'));
        return sameMatch || prefixMatch;
    }
}
