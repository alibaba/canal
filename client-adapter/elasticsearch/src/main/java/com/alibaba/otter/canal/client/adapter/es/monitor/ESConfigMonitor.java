package com.alibaba.otter.canal.client.adapter.es.monitor;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.client.adapter.config.YmlConfigBinder;
import com.alibaba.otter.canal.client.adapter.es.ESAdapter;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem;
import com.alibaba.otter.canal.client.adapter.es.config.SqlParser;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.MappingConfigsLoader;
import com.alibaba.otter.canal.client.adapter.support.Util;

public class ESConfigMonitor {

    private static final Logger   logger      = LoggerFactory.getLogger(ESConfigMonitor.class);

    private static final String   adapterName = "es";

    private ESAdapter             esAdapter;

    private Properties            envProperties;

    private FileAlterationMonitor fileMonitor;

    public void init(ESAdapter esAdapter, Properties envProperties) {
        this.esAdapter = esAdapter;
        this.envProperties = envProperties;
        File confDir = Util.getConfDirPath(adapterName);
        try {
            FileAlterationObserver observer = new FileAlterationObserver(confDir,
                FileFilterUtils.and(FileFilterUtils.fileFileFilter(), FileFilterUtils.suffixFileFilter("yml")));
            FileListener listener = new FileListener();
            observer.addListener(listener);
            fileMonitor = new FileAlterationMonitor(3000, observer);
            fileMonitor.start();

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void destroy() {
        try {
            fileMonitor.stop();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private class FileListener extends FileAlterationListenerAdaptor {

        @Override
        public void onFileCreate(File file) {
            super.onFileCreate(file);
            try {
                // 加载新增的配置文件
                String configContent = MappingConfigsLoader.loadConfig(adapterName + File.separator + file.getName());
                ESSyncConfig config = YmlConfigBinder
                    .bindYmlToObj(null, configContent, ESSyncConfig.class, null, envProperties);
                if (config != null) {
                    config.validate();
                    addConfigToCache(file, config);
                    logger.info("Add a new es mapping config: {} to canal adapter", file.getName());
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        @Override
        public void onFileChange(File file) {
            super.onFileChange(file);

            try {
                if (esAdapter.getEsSyncConfig().containsKey(file.getName())) {
                    // 加载配置文件
                    String configContent = MappingConfigsLoader
                        .loadConfig(adapterName + File.separator + file.getName());
                    if (configContent == null) {
                        onFileDelete(file);
                        return;
                    }
                    ESSyncConfig config = YmlConfigBinder
                        .bindYmlToObj(null, configContent, ESSyncConfig.class, null, envProperties);
                    if (config == null) {
                        return;
                    }
                    config.validate();
                    if (esAdapter.getEsSyncConfig().containsKey(file.getName())) {
                        deleteConfigFromCache(file);
                    }
                    addConfigToCache(file, config);

                    logger.info("Change a es mapping config: {} of canal adapter", file.getName());
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        @Override
        public void onFileDelete(File file) {
            super.onFileDelete(file);

            try {
                if (esAdapter.getEsSyncConfig().containsKey(file.getName())) {
                    deleteConfigFromCache(file);

                    logger.info("Delete a es mapping config: {} of canal adapter", file.getName());
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        private void addConfigToCache(File file, ESSyncConfig config) {
            esAdapter.getEsSyncConfig().put(file.getName(), config);

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

            schemaItem.getAliasTableItems().values().forEach(tableItem -> {
                Map<String, ESSyncConfig> esSyncConfigMap = esAdapter.getDbTableEsSyncConfig()
                    .computeIfAbsent(schema + "-" + tableItem.getTableName(), k -> new HashMap<>());
                esSyncConfigMap.put(file.getName(), config);
            });

        }

        private void deleteConfigFromCache(File file) {
            esAdapter.getEsSyncConfig().remove(file.getName());
            for (Map<String, ESSyncConfig> configMap : esAdapter.getDbTableEsSyncConfig().values()) {
                if (configMap != null) {
                    configMap.remove(file.getName());
                }
            }

        }
    }
}
