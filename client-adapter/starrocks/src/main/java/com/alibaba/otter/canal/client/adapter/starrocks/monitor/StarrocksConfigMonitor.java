package com.alibaba.otter.canal.client.adapter.starrocks.monitor;

import com.alibaba.otter.canal.client.adapter.starrocks.StarrocksAdapter;
import com.alibaba.otter.canal.client.adapter.starrocks.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.support.MappingConfigsLoader;
import com.alibaba.otter.canal.client.adapter.support.Util;
import com.alibaba.otter.canal.client.adapter.support.YamlUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

public class StarrocksConfigMonitor {
    private static final Logger logger      = LoggerFactory.getLogger(StarrocksConfigMonitor.class);

    private  StarrocksAdapter starrocksAdapter;

    private Properties envProperties;

    private static final String   adapterName = "starrocks";

    private  FileAlterationMonitor fileMonitor;

    public void init(StarrocksAdapter starrocksAdapter, Properties envProperties) {
        this.starrocksAdapter = starrocksAdapter;
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
                MappingConfig config = YamlUtils.ymlToObj(null, configContent, MappingConfig.class, null, envProperties);
                if (config == null) {
                    return;
                }
                config.validate();
                boolean result = starrocksAdapter.addConfig(file.getName(), config);
                if (result) {
                    logger.info("Add a new starrocks mapping config: {} to canal adapter",
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
                if (starrocksAdapter.getSrMapping().containsKey(file.getName())) {
                    // 加载配置文件
                    String configContent = MappingConfigsLoader
                            .loadConfig(adapterName + File.separator + file.getName());
                    if (configContent == null) {
                        onFileDelete(file);
                        return;
                    }
                    MappingConfig config = YamlUtils.ymlToObj(null, configContent, MappingConfig.class, null, envProperties);
                    if (config == null) {
                        return;
                    }
                    config.validate();
                    starrocksAdapter.updateConfig(file.getName(), config);
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        @Override
        public void onFileDelete(File file) {
            super.onFileDelete(file);

            try {
                if (starrocksAdapter.getSrMapping().containsKey(file.getName())) {
                    starrocksAdapter.deleteConfig(file.getName());
                    logger.info("Delete a starrocks mapping config: {} of canal adapter", file.getName());
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }
}
