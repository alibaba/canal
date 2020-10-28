package com.alibaba.otter.canal.client.adapter.hbase.monitor;

import com.alibaba.otter.canal.client.adapter.config.YmlConfigBinder;
import com.alibaba.otter.canal.client.adapter.hbase.HbaseAdapter;
import com.alibaba.otter.canal.client.adapter.hbase.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.support.MappingConfigsLoader;
import com.alibaba.otter.canal.client.adapter.support.Util;

import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class HbaseConfigMonitor {

    private static final Logger   logger      = LoggerFactory.getLogger(HbaseConfigMonitor.class);

    private static final String   adapterName = "hbase";

    private HbaseAdapter          hbaseAdapter;

    private Properties            envProperties;

    private FileAlterationMonitor fileMonitor;

    public void init(HbaseAdapter hbaseAdapter, Properties envProperties) {
        this.hbaseAdapter = hbaseAdapter;
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
                MappingConfig config = YmlConfigBinder
                    .bindYmlToObj(null, configContent, MappingConfig.class, null, envProperties);
                if (config == null) {
                    return;
                }
                config.validate();
                addConfigToCache(file, config);

                logger.info("Add a new hbase mapping config: {} to canal adapter", file.getName());
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        @Override
        public void onFileChange(File file) {
            super.onFileChange(file);

            try {
                if (hbaseAdapter.getHbaseMapping().containsKey(file.getName())) {
                    // 加载配置文件
                    String configContent = MappingConfigsLoader
                        .loadConfig(adapterName + File.separator + file.getName());
                    if (configContent == null) {
                        onFileDelete(file);
                        return;
                    }
                    MappingConfig config = YmlConfigBinder
                        .bindYmlToObj(null, configContent, MappingConfig.class, null, envProperties);
                    if (config == null) {
                        return;
                    }
                    config.validate();
                    if (hbaseAdapter.getHbaseMapping().containsKey(file.getName())) {
                        deleteConfigFromCache(file);
                    }
                    addConfigToCache(file, config);
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        @Override
        public void onFileDelete(File file) {
            super.onFileDelete(file);

            try {
                if (hbaseAdapter.getHbaseMapping().containsKey(file.getName())) {
                    deleteConfigFromCache(file);

                    logger.info("Delete a hbase mapping config: {} of canal adapter", file.getName());
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        private void addConfigToCache(File file, MappingConfig config) {
            hbaseAdapter.getHbaseMapping().put(file.getName(), config);
            Map<String, MappingConfig> configMap = hbaseAdapter.getMappingConfigCache()
                .computeIfAbsent(StringUtils.trimToEmpty(config.getDestination()) + "_"
                                 + config.getHbaseMapping().getDatabase() + "-" + config.getHbaseMapping().getTable(),
                    k1 -> new HashMap<>());
            configMap.put(file.getName(), config);
        }

        private void deleteConfigFromCache(File file) {

            hbaseAdapter.getHbaseMapping().remove(file.getName());
            for (Map<String, MappingConfig> configMap : hbaseAdapter.getMappingConfigCache().values()) {
                if (configMap != null) {
                    configMap.remove(file.getName());
                }
            }

        }
    }
}
