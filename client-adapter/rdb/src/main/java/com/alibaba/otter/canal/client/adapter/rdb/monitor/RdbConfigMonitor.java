package com.alibaba.otter.canal.client.adapter.rdb.monitor;

import com.alibaba.otter.canal.client.adapter.config.YmlConfigBinder;
import com.alibaba.otter.canal.client.adapter.rdb.RdbAdapter;
import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfig;
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

public class RdbConfigMonitor {

    private static final Logger   logger      = LoggerFactory.getLogger(RdbConfigMonitor.class);

    private static final String   adapterName = "rdb";

    private String                key;

    private RdbAdapter            rdbAdapter;

    private Properties            envProperties;

    private FileAlterationMonitor fileMonitor;

    public void init(String key, RdbAdapter rdbAdapter, Properties envProperties) {
        this.key = key;
        this.rdbAdapter = rdbAdapter;
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
                boolean result = rdbAdapter.addConfig(file.getName(), config);
                if (result) {
                    logger.info("Add a new rdb mapping config: {} to canal adapter",
                            file.getName());
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
                    if (configContent == null) {
                        onFileDelete(file);
                        return;
                    }
                    MappingConfig config = YmlConfigBinder
                            .bindYmlToObj(null, configContent, MappingConfig.class, null,
                                    envProperties);
                    if (config == null) {
                        return;
                    }
                    config.validate();
                    rdbAdapter.updateConfig(file.getName(), config);
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
                    rdbAdapter.deleteConfig(file.getName());

                    logger.info("Delete a rdb mapping config: {} of canal adapter", file.getName());
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }
}
