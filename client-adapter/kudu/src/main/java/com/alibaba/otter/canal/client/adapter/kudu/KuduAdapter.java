package com.alibaba.otter.canal.client.adapter.kudu;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.kudu.config.KuduMappingConfig;
import com.alibaba.otter.canal.client.adapter.kudu.config.KuduMappingConfigLoader;
import com.alibaba.otter.canal.client.adapter.kudu.monitor.KuduConfigMonitor;
import com.alibaba.otter.canal.client.adapter.kudu.service.KuduEtlService;
import com.alibaba.otter.canal.client.adapter.kudu.service.KuduSyncService;
import com.alibaba.otter.canal.client.adapter.kudu.support.KuduTemplate;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;
import com.alibaba.otter.canal.client.adapter.support.SPI;

/**
 * @author liuyadong
 * @description kudu适配器主类
 */
@SPI("kudu")
public class KuduAdapter implements OuterAdapter {

    private static Logger                               logger             = LoggerFactory.getLogger(KuduAdapter.class);

    private Map<String, KuduMappingConfig>              kuduMapping        = new ConcurrentHashMap<>();                 // 文件名对应配置
    private Map<String, Map<String, KuduMappingConfig>> mappingConfigCache = new ConcurrentHashMap<>();                 // 库名-表名对应配置

    private String                                      dataSourceKey;

    private KuduTemplate                                kuduTemplate;

    private KuduSyncService                             kuduSyncService;

    private KuduConfigMonitor                           kuduConfigMonitor;

    private Properties                                  envProperties;

    public Map<String, KuduMappingConfig> getKuduMapping() {
        return kuduMapping;
    }

    public Map<String, Map<String, KuduMappingConfig>> getMappingConfigCache() {
        return mappingConfigCache;
    }

    @Override
    public void init(OuterAdapterConfig configuration, Properties envProperties) {
        this.envProperties = envProperties;
        Map<String, KuduMappingConfig> kuduMappingTmp = KuduMappingConfigLoader.load(envProperties);
        // 过滤不匹配的key的配置,获取连接key，key为配置文件名称
        kuduMappingTmp.forEach((key, mappingConfig) -> {
            if ((mappingConfig.getOuterAdapterKey() == null && configuration.getKey() == null)
                || (mappingConfig.getOuterAdapterKey() != null && mappingConfig.getOuterAdapterKey()
                    .equalsIgnoreCase(configuration.getKey()))) {
                kuduMapping.put(key, mappingConfig);
                dataSourceKey = mappingConfig.getDataSourceKey();
            }
        });
        // 判断目标字段是否为空
        if (kuduMapping.isEmpty()) {
            throw new RuntimeException("No kudu adapter found for config key: " + configuration.getKey());
        }
        for (Map.Entry<String, KuduMappingConfig> entry : kuduMapping.entrySet()) {
            String configName = entry.getKey();
            KuduMappingConfig mappingConfig = entry.getValue();
            String k;
            if (envProperties != null && !"tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
                k = StringUtils.trimToEmpty(mappingConfig.getDestination()) + "-"
                    + StringUtils.trimToEmpty(mappingConfig.getGroupId()) + "_"
                    + mappingConfig.getKuduMapping().getDatabase() + "-" + mappingConfig.getKuduMapping().getTable();
            } else {
                k = StringUtils.trimToEmpty(mappingConfig.getDestination()) + "_"
                    + mappingConfig.getKuduMapping().getDatabase() + "-" + mappingConfig.getKuduMapping().getTable();
            }
            Map<String, KuduMappingConfig> configMap = mappingConfigCache.computeIfAbsent(k,
                k1 -> new ConcurrentHashMap<>());
            configMap.put(configName, mappingConfig);
        }
        Map<String, String> properties = configuration.getProperties();

        String kudu_master = properties.get("kudu.master.address");
        kuduTemplate = new KuduTemplate(kudu_master);
        kuduSyncService = new KuduSyncService(kuduTemplate);

        kuduConfigMonitor = new KuduConfigMonitor();
        kuduConfigMonitor.init(this, envProperties);
    }

    @Override
    public void sync(List<Dml> dmls) {
        if (dmls == null || dmls.isEmpty()) {
            return;
        }
        for (Dml dml : dmls) {
            if (dml == null) {
                return;
            }
            String destination = StringUtils.trimToEmpty(dml.getDestination());
            String groupId = StringUtils.trimToEmpty(dml.getGroupId());
            String database = dml.getDatabase();
            String table = dml.getTable();
            Map<String, KuduMappingConfig> configMap;
            if (envProperties != null && !"tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
                configMap = mappingConfigCache.get(destination + "-" + groupId + "_" + database + "-" + table);
            } else {
                configMap = mappingConfigCache.get(destination + "_" + database + "-" + table);
            }
            if (configMap != null) {
                List<KuduMappingConfig> configs = new ArrayList<>();
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
                    configs.forEach(config -> kuduSyncService.sync(config, dml));
                } else {
                    logger.error("groupID didn't mach,please check your gruopId ");
                }
            } else {
                logger.error("{} config didn't get,please check your map key ", destination + "_" + database + "-"
                                                                                + table);
            }
        }
    }

    @Override
    public void destroy() {
        if (kuduConfigMonitor != null) {
            kuduConfigMonitor.destroy();
        }
        // 加入kudu client 关闭钩子
        kuduTemplate.closeKuduClient();
    }

    @Override
    public EtlResult etl(String task, List<String> params) {
        EtlResult etlResult = new EtlResult();
        KuduMappingConfig config = kuduMapping.get(task);
        KuduEtlService hbaseEtlService = new KuduEtlService(kuduTemplate, config);
        if (config != null) {
            return hbaseEtlService.importData(params);
        } else {
            StringBuilder resultMsg = new StringBuilder();
            boolean resSucc = true;
            for (KuduMappingConfig configTmp : kuduMapping.values()) {
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
        return null;
    }

    @Override
    public Map<String, Object> count(String task) {
        Map<String, Object> res = new LinkedHashMap<>();
        KuduMappingConfig config = kuduMapping.get(task);
        if (config != null && config.getKuduMapping() != null) {
            String tableName = config.getKuduMapping().getTargetTable();
            long rowCount = kuduTemplate.countRow(tableName);
            res.put("kuduTable", tableName);
            res.put("count", rowCount);
        }
        return res;
    }

    @Override
    public String getDestination(String task) {
        KuduMappingConfig config = kuduMapping.get(task);
        if (config != null && config.getKuduMapping() != null) {
            return config.getDestination();
        }
        return null;
    }
}
