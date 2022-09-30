package com.alibaba.otter.canal.client.adapter.starrocks;

import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.starrocks.config.ConfigLoader;
import com.alibaba.otter.canal.client.adapter.starrocks.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.starrocks.monitor.StarrocksConfigMonitor;
import com.alibaba.otter.canal.client.adapter.starrocks.service.StarrocksSyncService;
import com.alibaba.otter.canal.client.adapter.starrocks.support.StarrocksTemplate;
import com.alibaba.otter.canal.client.adapter.support.*;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * StarRocks适配器实现类
 */

@SPI("starrocks")
public class StarrocksAdapter implements OuterAdapter {

    private Map<String, MappingConfig> srMapping = new ConcurrentHashMap<>();
    private Map<String, Map<String, MappingConfig>> mappingConfigCache  = new ConcurrentHashMap<>();
    private Properties                              envProperties;

    private OuterAdapterConfig                      configuration;

    private StarrocksSyncService starrocksSyncService;

    private StarrocksTemplate starrocksTemplate;

    protected StarrocksConfigMonitor starrocksConfigMonitor;

    public Map<String, MappingConfig> getSrMapping() {
        return srMapping;
    }

    @Override
    public void init(OuterAdapterConfig configuration, Properties envProperties) {
        try {
            this.envProperties = envProperties;
            this.configuration = configuration;
            Map<String, MappingConfig> srMappingConfigMap = ConfigLoader.load(envProperties);
            srMappingConfigMap.forEach(this::addConfig);

            if (srMapping.isEmpty()) {
                throw new RuntimeException("No starrocks adapter found for config key: " + configuration.getKey());
            }

            Map<String, String> properties = configuration.getProperties();
            String jdbcUrl = properties.get("jdbc.url");
            String loadUrl = properties.get("load.url");
            String userName = properties.get("user.name");
            String password = properties.get("user.password");
            String dataBaseName = properties.get("database.name");
            starrocksTemplate = new StarrocksTemplate(jdbcUrl, loadUrl, userName, password, dataBaseName);
            starrocksSyncService = new StarrocksSyncService(starrocksTemplate);

            starrocksConfigMonitor = new StarrocksConfigMonitor();
            starrocksConfigMonitor.init(this, envProperties);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void sync(List<Dml> dmls) {
        if (dmls == null || dmls.isEmpty()) {
            return;
        }

        try {
            if (!mappingConfigCache.isEmpty()) {
                starrocksSyncService.sync(mappingConfigCache, dmls, envProperties);
            }
        }catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void destroy() {
        if(starrocksConfigMonitor != null) {
            starrocksConfigMonitor.destroy();
        }
    }

    @Override
    public EtlResult etl(String task, List<String> params) {
        return OuterAdapter.super.etl(task, params);
    }

    @Override
    public Map<String, Object> count(String task) {
        return OuterAdapter.super.count(task);
    }

    @Override
    public String getDestination(String task) {
        return OuterAdapter.super.getDestination(task);
    }

    private void addSyncConfigToCache(String configName, MappingConfig mappingConfig) {
        String k;
        if (envProperties != null && !"tcp"
                .equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
            k = StringUtils.trimToEmpty(mappingConfig.getDestination()) + "-" + StringUtils
                    .trimToEmpty(mappingConfig.getGroupId()) + "_" + mappingConfig.getSrMapping()
                    .getDatabase() + "-" + mappingConfig.getSrMapping().getTable();
        } else {
            k = StringUtils.trimToEmpty(mappingConfig.getDestination()) + "_" + mappingConfig
                    .getSrMapping().getDatabase() + "-" + mappingConfig.getSrMapping()
                    .getTable();
        }

        Map<String, MappingConfig> configMap = mappingConfigCache.computeIfAbsent(k,
                k1 -> new ConcurrentHashMap<>());

        configMap.put(configName, mappingConfig);
    }

    public void updateConfig(String fileName, MappingConfig config) {
        if (config.getOuterAdapterKey() != null && !config.getOuterAdapterKey()
                .equals(configuration.getKey())) {
            // 理论上不允许改这个 因为本身就是通过这个关联起Adapter和Config的
            throw new RuntimeException("not allow to change outAdapterKey");
        }
        srMapping.put(fileName, config);
        addSyncConfigToCache(fileName, config);
    }

    public void deleteConfig(String fileName) {
        srMapping.remove(fileName);
        for (Map<String, MappingConfig> configMap : mappingConfigCache.values()) {
            if (configMap != null) {
                configMap.remove(fileName);
            }
        }
        FileName2KeyMapping.unregister(getClass().getAnnotation(SPI.class).value(), fileName);
    }

    public boolean addConfig(String fileName, MappingConfig config) {
        if (match(config)) {
            srMapping.put(fileName, config);
            addSyncConfigToCache(fileName, config);
            FileName2KeyMapping.register(getClass().getAnnotation(SPI.class).value(), fileName,
                    configuration.getKey());
            return true;
        }
        return false;
    }

    private boolean match(MappingConfig config) {
        boolean sameMatch = config.getOuterAdapterKey() != null && config.getOuterAdapterKey()
                .equalsIgnoreCase(configuration.getKey());
        boolean prefixMatch = config.getOuterAdapterKey()==null && configuration.getKey()
                .startsWith(StringUtils.join(new String[]{Util.AUTO_GENERATED_PREFIX, config.getDestination(),
                        config.getGroupId()}, '-'));
        return sameMatch || prefixMatch;
    }
}
