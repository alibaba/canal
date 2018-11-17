package com.alibaba.otter.canal.client.adapter.rdb.monitor;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.alibaba.otter.canal.client.adapter.rdb.RdbAdapter;
import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.support.MappingConfigsLoader;
import com.alibaba.otter.canal.client.adapter.support.Util;

public class RdbConfigMonitor {

    private static final Logger   logger = LoggerFactory.getLogger(RdbConfigMonitor.class);

    private static final String   adapterName = "rdb";

    private String                key;

    private RdbAdapter            rdbAdapter;

    private FileAlterationMonitor fileMonitor;

    public void init(String key, RdbAdapter rdbAdapter) {
        this.key = key;
        this.rdbAdapter = rdbAdapter;
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
                MappingConfig config = new Yaml().loadAs(configContent, MappingConfig.class);
                config.validate();
                if ((key == null && config.getOuterAdapterKey() == null)
                    || (key != null && key.equals(config.getOuterAdapterKey()))) {
                    addConfigToCache(file, config);

                    logger.info("Add a new rdb mapping config: {} to canal adapter", file.getName());
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        @Override
        public void onFileChange(File file) {
            super.onFileChange(file);

            try {
                if (rdbAdapter.getRdbMapping().containsKey(file.getName())) {
                    // 加载配置文件
                    String configContent = MappingConfigsLoader.loadConfig(adapterName + File.separator + file.getName());
                    MappingConfig config = new Yaml().loadAs(configContent, MappingConfig.class);
                    config.validate();
                    if ((key == null && config.getOuterAdapterKey() == null)
                        || (key != null && key.equals(config.getOuterAdapterKey()))) {
                        if (rdbAdapter.getRdbMapping().containsKey(file.getName())) {
                            deleteConfigFromCache(file);
                        }
                        addConfigToCache(file, config);
                    } else {
                        // 不能修改outerAdapterKey
                        throw new RuntimeException("Outer adapter key not allowed modify");
                    }
                    logger.info("Change a rdb mapping config: {} of canal adapter", file.getName());
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        @Override
        public void onFileDelete(File file) {
            super.onFileDelete(file);

            try {
                if (rdbAdapter.getRdbMapping().containsKey(file.getName())) {
                    deleteConfigFromCache(file);

                    logger.info("Delete a rdb mapping config: {} of canal adapter", file.getName());
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        private void addConfigToCache(File file, MappingConfig config) {
            rdbAdapter.getRdbMapping().put(file.getName(), config);
            Map<String, MappingConfig> configMap = rdbAdapter.getMappingConfigCache()
                .computeIfAbsent(StringUtils.trimToEmpty(config.getDestination()) + "."
                                 + config.getDbMapping().getDatabase() + "." + config.getDbMapping().getTable(),
                    k1 -> new HashMap<>());
            configMap.put(file.getName(), config);
        }

        private void deleteConfigFromCache(File file) {

            rdbAdapter.getRdbMapping().remove(file.getName());
            for (Map<String, MappingConfig> configMap : rdbAdapter.getMappingConfigCache().values()) {
                if (configMap != null) {
                    configMap.remove(file.getName());
                }
            }

        }
    }
}
