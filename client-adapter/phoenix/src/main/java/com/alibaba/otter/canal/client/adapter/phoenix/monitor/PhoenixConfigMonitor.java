package com.alibaba.otter.canal.client.adapter.phoenix.monitor;

import com.alibaba.otter.canal.client.adapter.config.YmlConfigBinder;
import com.alibaba.otter.canal.client.adapter.phoenix.PhoenixAdapter;
import com.alibaba.otter.canal.client.adapter.phoenix.config.MappingConfig;
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
                boolean result = phoenixAdapter.addConfig(file.getName(), config);
                if (result) {
                    logger.info("Add a new phoenix mapping config: {} to canal adapter",
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
                    phoenixAdapter.updateConfig(file.getName(), config);
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
                    phoenixAdapter.deleteConfig(file.getName());
                    logger.info("Delete a phoenix mapping config: {} of canal adapter", file.getName());
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }
}
