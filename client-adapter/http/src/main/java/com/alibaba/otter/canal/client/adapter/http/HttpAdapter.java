/**
 * Created by Wu Jian Ping on - 2021/09/15.
 */

package com.alibaba.otter.canal.client.adapter.http;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.StringUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;
import com.alibaba.otter.canal.client.adapter.support.SPI;

import com.alibaba.otter.canal.client.adapter.http.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.http.config.MappingConfigLoader;
import com.alibaba.otter.canal.client.adapter.http.config.MappingConfig.MonitorTable;
import com.alibaba.otter.canal.client.adapter.http.monitor.HttpConfigMonitor;
import com.alibaba.otter.canal.client.adapter.http.support.HttpTemplate;
import com.alibaba.otter.canal.client.adapter.http.service.HttpEtlService;
import com.alibaba.otter.canal.client.adapter.http.service.HttpSyncService;

@SPI("http")
public class HttpAdapter implements OuterAdapter {

    private static Logger logger = LoggerFactory.getLogger(HttpAdapter.class);

    private Map<String, MappingConfig> mappingConfig = new ConcurrentHashMap<>();
    private Map<String, Map<String, MappingConfig>> mappingConfigCache = new ConcurrentHashMap<>();

    private HttpTemplate httpTemplate;

    private HttpSyncService httpSyncService;

    private HttpConfigMonitor configMonitor;

    private Properties envProperties;

    public Map<String, MappingConfig> getHttpMapping() {
        return this.mappingConfig;
    }

    public Map<String, Map<String, MappingConfig>> getMappingConfigCache() {
        return mappingConfigCache;
    }

    @Override
    public void init(OuterAdapterConfig configuration, Properties envProperties) {
        if (logger.isDebugEnabled()) {
            logger.debug("init http adapter");
            logger.debug("OuterAdapterConfig: {}", JSON.toJSONString(configuration));
        }

        Map<String, String> properties = configuration.getProperties();
        String serviceUrl = properties.get("serviceUrl");
        String sign = properties.get("sign");

        try {
            this.envProperties = envProperties;
            Map<String, MappingConfig> httpMappingTmp = MappingConfigLoader.load(envProperties);

            // 这边的key是http目录下的fileName
            httpMappingTmp.forEach((key, config) -> {
                if ((config.getOuterAdapterKey() == null && configuration.getKey() == null)
                        || (config.getOuterAdapterKey() != null
                                && config.getOuterAdapterKey().equalsIgnoreCase(configuration.getKey()))) {

                    this.mappingConfig.put(key, config);
                }
            });

            for (Map.Entry<String, MappingConfig> entry : this.mappingConfig.entrySet()) {
                String configName = entry.getKey();
                MappingConfig mappingConfig = entry.getValue();
                String k;
                if (envProperties != null && !"tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
                    k = StringUtils.trimToEmpty(mappingConfig.getDestination()) + "-"
                            + StringUtils.trimToEmpty(mappingConfig.getGroupId());
                } else {
                    k = StringUtils.trimToEmpty(mappingConfig.getDestination());
                }
                Map<String, MappingConfig> configMap = mappingConfigCache.computeIfAbsent(k,
                        k1 -> new ConcurrentHashMap<>());
                configMap.put(configName, mappingConfig);

            }

            if (logger.isDebugEnabled()) {
                logger.debug("mappingConfig: {}", JSON.toJSONString(this.mappingConfig));
                logger.debug("mappingConfigCache: {}", JSON.toJSONString(this.mappingConfigCache));
            }

            this.httpTemplate = new HttpTemplate(serviceUrl, sign);
            this.httpSyncService = new HttpSyncService(this.httpTemplate);

            this.configMonitor = new HttpConfigMonitor();
            this.configMonitor.init(this, envProperties);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void sync(List<Dml> dmls) {
        if (dmls == null || dmls.isEmpty()) {
            return;
        }

        Map<String, List<Dml>> validDmls = new ConcurrentHashMap<>();
        Map<String, MappingConfig> configMaps = new ConcurrentHashMap<>();

        for (Dml dml : dmls) {
            if (dml == null) {
                continue;
            }

            String destination = StringUtils.trimToEmpty(dml.getDestination());
            String groupId = StringUtils.trimToEmpty(dml.getGroupId());

            Map<String, MappingConfig> configMap;
            if (envProperties != null && !"tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
                configMap = mappingConfigCache.get(destination + "-" + groupId);
            } else {
                configMap = mappingConfigCache.get(destination);
            }

            if (configMap != null) {
                for (String key : configMap.keySet()) {
                    MappingConfig config = configMap.get(key);
                    boolean matchGroup = false;
                    if (StringUtils.isNotEmpty(config.getGroupId())) {
                        if (config.getGroupId().equals(dml.getGroupId())) {
                            matchGroup = true;
                        }
                    } else {
                        matchGroup = true;
                    }

                    if (matchGroup) {
                        // 这边根据monitorTables决定是否需要同步
                        String tableName = dml.getDatabase() + "." + dml.getTable();
                        List<MonitorTable> monitorTables = config.getHttpMapping().getMonitorTables();
                        boolean shouldSync = false;

                        for (MonitorTable monitorTable : monitorTables) {
                            // 假如当前表在配置的监控表列表中
                            if (monitorTable.getTableName().equalsIgnoreCase(tableName)) {
                                List<String> actions = monitorTable.getActions();
                                // 假如没有配置action, 则认为需要同步
                                if (actions == null || actions.size() == 0) {
                                    shouldSync = true;
                                } else {
                                    // 假如配置的actions, 则需要看一下当前action是否在列表中
                                    for (String action : actions) {
                                        if (action.equalsIgnoreCase(dml.getType())) {
                                            shouldSync = true;
                                            break;
                                        }
                                    }
                                }
                            }

                            // 只要命中一个就跳出循环
                            if (shouldSync) {
                                break;
                            }
                        }

                        if (shouldSync) {
                            if (!validDmls.containsKey(key)) {
                                validDmls.put(key, new ArrayList<Dml>());
                            }
                            List<Dml> list = validDmls.get(key);
                            list.add(dml);

                            if (!configMaps.containsKey(key)) {
                                configMaps.put(key, config);
                            }
                        }
                    }
                }
            }
        }

        for (String key : validDmls.keySet()) {
            this.httpSyncService.sync(configMaps.get(key), validDmls.get(key));
        }
    }

    @Override
    public EtlResult etl(String task, List<String> params) {
        EtlResult etlResult = new EtlResult();
        MappingConfig config = this.mappingConfig.get(task);
        HttpEtlService httpEtlService = new HttpEtlService(this.httpTemplate, config);
        if (config != null) {
            return httpEtlService.importData(params);
        } else {
            StringBuilder resultMsg = new StringBuilder();
            boolean resSucc = true;
            for (MappingConfig configTmp : this.mappingConfig.values()) {
                // 取所有的destination为task的配置
                if (configTmp.getDestination().equals(task)) {
                    EtlResult etlRes = httpEtlService.importData(params);
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
        int count = this.httpTemplate.count();
        Map<String, Object> res = new LinkedHashMap<>();
        res.put("count", count);
        return res;
    }

    @Override
    public String getDestination(String task) {
        MappingConfig config = mappingConfig.get(task);
        if (config != null && config.getHttpMapping() != null) {
            return config.getDestination();
        }
        return null;
    }

    @Override
    public void destroy() {
        if (this.configMonitor != null) {
            this.configMonitor.destroy();
        }
    }
}
