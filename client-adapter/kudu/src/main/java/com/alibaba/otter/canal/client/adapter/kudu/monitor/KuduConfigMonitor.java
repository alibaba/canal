package com.alibaba.otter.canal.client.adapter.kudu.monitor;

import com.alibaba.otter.canal.client.adapter.config.YmlConfigBinder;
import com.alibaba.otter.canal.client.adapter.kudu.KuduAdapter;
import com.alibaba.otter.canal.client.adapter.kudu.config.KuduMappingConfig;
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
 * @author liuyadong
 * @description 配置文件监听
 */
public class KuduConfigMonitor {

    private static final Logger   logger      = LoggerFactory.getLogger(KuduConfigMonitor.class);

    private static final String   adapterName = "kudu";

    private KuduAdapter           kuduAdapter;

    private Properties            envProperties;

    private FileAlterationMonitor fileMonitor;

    public void init(KuduAdapter kuduAdapter, Properties envProperties) {
        this.kuduAdapter = kuduAdapter;
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

    /**
     * 停止监听配置文件
     */
    public void destroy() {
        try {
            fileMonitor.stop();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 配置文件监听
     */
    private class FileListener extends FileAlterationListenerAdaptor {

        @Override
        public void onFileCreate(File file) {
            super.onFileCreate(file);

            try {
                // 加载新增的配置文件
                String configContent = MappingConfigsLoader.loadConfig(adapterName + File.separator + file.getName());
                KuduMappingConfig config = YmlConfigBinder.bindYmlToObj(null,
                    configContent,
                    KuduMappingConfig.class,
                    null,
                    envProperties);
                if (config == null) {
                    return;
                }
                config.validate();
                boolean result = kuduAdapter.addConfig(file.getName(), config);
                if (result) {
                    logger.info("Add a new kudu mapping config: {} to canal adapter",
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
                if (kuduAdapter.getKuduMapping().containsKey(file.getName())) {
                    // 加载配置文件
                    String configContent = MappingConfigsLoader.loadConfig(adapterName + File.separator
                                                                           + file.getName());
                    if (configContent == null) {
                        onFileDelete(file);
                        return;
                    }
                    KuduMappingConfig config = YmlConfigBinder.bindYmlToObj(null,
                        configContent,
                        KuduMappingConfig.class,
                        null,
                        envProperties);
                    if (config == null) {
                        return;
                    }
                    config.validate();
                    kuduAdapter.updateConfig(file.getName(), config);
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        @Override
        public void onFileDelete(File file) {
            super.onFileDelete(file);

            try {
                if (kuduAdapter.getKuduMapping().containsKey(file.getName())) {
                    kuduAdapter.deleteConfig(file.getName());
                    logger.info("Delete a hbase mapping config: {} of canal adapter", file.getName());
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }
}
