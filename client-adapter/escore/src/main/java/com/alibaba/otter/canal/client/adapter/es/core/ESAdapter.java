package com.alibaba.otter.canal.client.adapter.es.core;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.es.core.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.core.config.ESSyncConfigLoader;
import com.alibaba.otter.canal.client.adapter.es.core.config.SchemaItem;
import com.alibaba.otter.canal.client.adapter.es.core.config.SqlParser;
import com.alibaba.otter.canal.client.adapter.es.core.monitor.ESConfigMonitor;
import com.alibaba.otter.canal.client.adapter.es.core.service.ESSyncService;
import com.alibaba.otter.canal.client.adapter.es.core.support.ESTemplate;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;

/**
 * ES外部适配器
 *
 * @author rewerma 2018-10-20
 * @version 1.0.0
 */
public abstract class ESAdapter implements OuterAdapter {

    protected Map<String, ESSyncConfig>              esSyncConfig        = new ConcurrentHashMap<>(); // 文件名对应配置
    protected Map<String, Map<String, ESSyncConfig>> dbTableEsSyncConfig = new ConcurrentHashMap<>(); // schema-table对应配置

    protected ESTemplate                             esTemplate;

    protected ESSyncService                          esSyncService;

    protected ESConfigMonitor                        esConfigMonitor;

    protected Properties                             envProperties;

    public ESSyncService getEsSyncService() {
        return esSyncService;
    }

    public Map<String, ESSyncConfig> getEsSyncConfig() {
        return esSyncConfig;
    }

    public Map<String, Map<String, ESSyncConfig>> getDbTableEsSyncConfig() {
        return dbTableEsSyncConfig;
    }

    @Override
    public void init(OuterAdapterConfig configuration, Properties envProperties) {
        try {
            this.envProperties = envProperties;
            Map<String, ESSyncConfig> esSyncConfigTmp = ESSyncConfigLoader.load(envProperties);
            // 过滤不匹配的key的配置
            esSyncConfigTmp.forEach((key, config) -> {
                if ((config.getOuterAdapterKey() == null && configuration.getKey() == null)
                    || (config.getOuterAdapterKey() != null && config.getOuterAdapterKey()
                        .equalsIgnoreCase(configuration.getKey()))) {
                    esSyncConfig.put(key, config);
                }
            });

            for (Map.Entry<String, ESSyncConfig> entry : esSyncConfig.entrySet()) {
                String configName = entry.getKey();
                ESSyncConfig config = entry.getValue();

                addSyncConfigToCache(configName, config);
            }

            esSyncService = new ESSyncService(esTemplate);

            esConfigMonitor = new ESConfigMonitor();
            esConfigMonitor.init(this, envProperties);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void sync(List<Dml> dmls) {
        if (dmls == null || dmls.isEmpty()) {
            return;
        }
        for (Dml dml : dmls) {
            if (!dml.getIsDdl()) {
                sync(dml);
            }
        }
        esSyncService.commit(); // 批次统一提交

    }

    private void sync(Dml dml) {
        String database = dml.getDatabase();
        String table = dml.getTable();
        Map<String, ESSyncConfig> configMap;
        if (envProperties != null && !"tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
            configMap = dbTableEsSyncConfig.get(StringUtils.trimToEmpty(dml.getDestination()) + "-"
                                                + StringUtils.trimToEmpty(dml.getGroupId()) + "_" + database + "-"
                                                + table);
        } else {
            configMap = dbTableEsSyncConfig.get(StringUtils.trimToEmpty(dml.getDestination()) + "_" + database + "-"
                                                + table);
        }

        if (configMap != null && !configMap.values().isEmpty()) {
            esSyncService.sync(configMap.values(), dml);
        }
    }

    @Override
    public abstract EtlResult etl(String task, List<String> params);

    @Override
    public abstract Map<String, Object> count(String task);

    @Override
    public void destroy() {
        if (esConfigMonitor != null) {
            esConfigMonitor.destroy();
        }
    }

    @Override
    public String getDestination(String task) {
        ESSyncConfig config = esSyncConfig.get(task);
        if (config != null) {
            return config.getDestination();
        }
        return null;
    }

    public void addSyncConfigToCache(String configName, ESSyncConfig config) {
        Properties envProperties = this.envProperties;
        SchemaItem schemaItem = SqlParser.parse(config.getEsMapping().getSql());
        config.getEsMapping().setSchemaItem(schemaItem);

        DruidDataSource dataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        if (dataSource == null || dataSource.getUrl() == null) {
            throw new RuntimeException("No data source found: " + config.getDataSourceKey());
        }
        Pattern pattern = Pattern.compile(".*:(.*)://.*/(.*)\\?.*$");
        Matcher matcher = pattern.matcher(dataSource.getUrl());
        if (!matcher.find()) {
            throw new RuntimeException("Not found the schema of jdbc-url: " + config.getDataSourceKey());
        }
        String schema = matcher.group(2);

        schemaItem.getAliasTableItems()
            .values()
            .forEach(tableItem -> {
                Map<String, ESSyncConfig> esSyncConfigMap;
                if (envProperties != null && !"tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
                    esSyncConfigMap = dbTableEsSyncConfig.computeIfAbsent(StringUtils.trimToEmpty(config.getDestination())
                                                                          + "-"
                                                                          + StringUtils.trimToEmpty(config.getGroupId())
                                                                          + "_"
                                                                          + schema
                                                                          + "-"
                                                                          + tableItem.getTableName(),
                        k -> new ConcurrentHashMap<>());
                } else {
                    esSyncConfigMap = dbTableEsSyncConfig.computeIfAbsent(StringUtils.trimToEmpty(config.getDestination())
                                                                          + "_"
                                                                          + schema
                                                                          + "-"
                                                                          + tableItem.getTableName(),
                        k -> new ConcurrentHashMap<>());
                }

                esSyncConfigMap.put(configName, config);
            });
    }
}
