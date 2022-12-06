package com.alibaba.otter.canal.client.adapter.es.core.monitor;

import com.alibaba.otter.canal.client.adapter.config.YmlConfigBinder;
import com.alibaba.otter.canal.client.adapter.es.core.ESAdapter;
import com.alibaba.otter.canal.client.adapter.es.core.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.support.MappingConfigsLoader;
import com.alibaba.otter.canal.client.adapter.support.Util;
import java.io.File;
import java.util.Properties;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ESConfigMonitor {

    private static final Logger   logger = LoggerFactory.getLogger(ESConfigMonitor.class);

    private String                adapterName;

    private ESAdapter             esAdapter;

    private Properties            envProperties;

    private FileAlterationMonitor fileMonitor;

    public void init(ESAdapter esAdapter, Properties envProperties) {
        this.esAdapter = esAdapter;
        this.envProperties = envProperties;
        this.adapterName = envProperties.getProperty("es.version");
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
                ESSyncConfig config = YmlConfigBinder.bindYmlToObj(null,
                    configContent,
                    ESSyncConfig.class,
                    null,
                    envProperties);
                if (config != null) {
                    // 这里要记得设置esVersion bugfix
                    config.setEsVersion(adapterName);
                    config.validate();
                    boolean result = esAdapter.addConfig(file.getName(), config);
                    if (result) {
                        logger.info("Add a new es mapping config: {} to canal adapter",
                                file.getName());
                    }
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
                    String configContent = MappingConfigsLoader.loadConfig(adapterName + File.separator
                                                                           + file.getName());
                    if (configContent == null) {
                        onFileDelete(file);
                        return;
                    }
                    ESSyncConfig config = YmlConfigBinder.bindYmlToObj(null,
                        configContent,
                        ESSyncConfig.class,
                        null,
                        envProperties);
                    if (config == null) {
                        return;
                    }
                    // 这里要记得设置esVersion bugfix
                    config.setEsVersion(adapterName);
                    config.validate();
                    esAdapter.updateConfig(file.getName(), config);
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
                    esAdapter.deleteConfig(file.getName());
                    logger.info("Delete a es mapping config: {} of canal adapter", file.getName());
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }
}
