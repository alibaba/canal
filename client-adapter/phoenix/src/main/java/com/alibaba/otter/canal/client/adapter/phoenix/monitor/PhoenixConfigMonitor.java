package com.alibaba.otter.canal.client.adapter.phoenix.monitor;

import com.alibaba.otter.canal.client.adapter.phoenix.PhoenixAdapter;
import com.alibaba.otter.canal.client.adapter.phoenix.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.config.YmlConfigBinder;
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

/**
 * phoenix config monitor
 */
public class PhoenixConfigMonitor {

    private static final Logger logger = LoggerFactory.getLogger(PhoenixConfigMonitor.class);

    private static final String adapterName = "phoenix";  //相应组件名字

    private String key;

    private PhoenixAdapter phoenixAdapter;   //相应适配器名实现类

    private Properties envProperties;

    private FileAlterationMonitor fileMonitor;

    public void init(String key, PhoenixAdapter phoenixAdapter, Properties envProperties) {
        this.key = key;
        this.phoenixAdapter = phoenixAdapter;
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
                if ((key == null && config.getOuterAdapterKey() == null)
                        || (key != null && key.equals(config.getOuterAdapterKey()))) {
                    addConfigToCache(file, config);

                    logger.info("Add a new phoenix mapping config: {} to canal adapter", file.getName());
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        @Override
        public void onFileChange(File file) {
            super.onFileChange(file);

            try {
                if (phoenixAdapter.getPhoenixMapping().containsKey(file.getName())) {
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
                    if ((key == null && config.getOuterAdapterKey() == null)
                            || (key != null && key.equals(config.getOuterAdapterKey()))) {
                        if (phoenixAdapter.getPhoenixMapping().containsKey(file.getName())) {
                            deleteConfigFromCache(file);
                        }
                        addConfigToCache(file, config);
                    } else {
                        // 不能修改outerAdapterKey
                        throw new RuntimeException("Outer adapter key not allowed modify");
                    }
                    logger.info("Change a phoenix mapping config: {} of canal adapter", file.getName());
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        @Override
        public void onFileDelete(File file) {
            super.onFileDelete(file);

            try {
                if (phoenixAdapter.getPhoenixMapping().containsKey(file.getName())) {
                    deleteConfigFromCache(file);

                    logger.info("Delete a phoenix mapping config: {} of canal adapter", file.getName());
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        private void addConfigToCache(File file, MappingConfig mappingConfig) {
            if (mappingConfig == null || mappingConfig.getDbMapping() == null) {
                return;
            }
            phoenixAdapter.getPhoenixMapping().put(file.getName(), mappingConfig);
            Map<String, MappingConfig> configMap = phoenixAdapter.getMappingConfigCache()
                    .computeIfAbsent(StringUtils.trimToEmpty(mappingConfig.getDestination()) + "_"
                                    + mappingConfig.getDbMapping().getDatabase() + "-"
                                    + mappingConfig.getDbMapping().getTable().toLowerCase(),
                            k1 -> new HashMap<>());
            configMap.put(file.getName(), mappingConfig);
        }

        private void deleteConfigFromCache(File file) {
            logger.info("deleteConfigFromCache: {}", file.getName());
            MappingConfig mappingConfig = phoenixAdapter.getPhoenixMapping().remove(file.getName());

            if (mappingConfig == null || mappingConfig.getDbMapping() == null) {
                return;
            }
            for (Map<String, MappingConfig> configMap : phoenixAdapter.getMappingConfigCache().values()) {
                if (configMap != null) {
                    configMap.remove(file.getName());
                }
            }
        }
    }
}
