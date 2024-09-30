package com.alibaba.otter.canal.client.adapter.clickhouse.monitor;

import java.io.File;
import java.util.Properties;

import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.adapter.clickhouse.ClickHouseAdapter;
import com.alibaba.otter.canal.client.adapter.clickhouse.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.support.MappingConfigsLoader;
import com.alibaba.otter.canal.client.adapter.support.Util;
import com.alibaba.otter.canal.client.adapter.support.YamlUtils;

/**
 * @author: Xander
 * @date: Created in 2023/11/10 22:23
 * @email: zhrunxin33@gmail.com
 * @version 1.1.8
 */

public class ClickHouseConfigMonitor {

    private static final Logger   logger      = LoggerFactory.getLogger(ClickHouseConfigMonitor.class);

    private static final String   adapterName = "clickhouse";

    private String                key;

    private ClickHouseAdapter     clickHouseAdapter;

    private Properties            envProperties;

    private FileAlterationMonitor fileMonitor;

    public void init(String key, ClickHouseAdapter clickHouseAdapter, Properties envProperties) {
        this.key = key;
        this.clickHouseAdapter = clickHouseAdapter;
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
                MappingConfig config = YamlUtils
                    .ymlToObj(null, configContent, MappingConfig.class, null, envProperties);
                if (config == null) {
                    return;
                }
                config.validate();
                boolean result = clickHouseAdapter.addConfig(file.getName(), config);
                if (result) {
                    logger.info("Add a new clickhouse mapping config: {} to canal adapter", file.getName());
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        @Override
        public void onFileChange(File file) {
            super.onFileChange(file);

            try {
                if (clickHouseAdapter.getClickHouseMapping().containsKey(file.getName())) {
                    // 加载配置文件
                    String configContent = MappingConfigsLoader
                        .loadConfig(adapterName + File.separator + file.getName());
                    if (configContent == null) {
                        onFileDelete(file);
                        return;
                    }
                    MappingConfig config = YamlUtils
                        .ymlToObj(null, configContent, MappingConfig.class, null, envProperties);
                    if (config == null) {
                        return;
                    }
                    config.validate();
                    clickHouseAdapter.updateConfig(file.getName(), config);
                    logger.info("Change a clickhouse mapping config: {} of canal adapter", file.getName());
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        @Override
        public void onFileDelete(File file) {
            super.onFileDelete(file);

            try {
                if (clickHouseAdapter.getClickHouseMapping().containsKey(file.getName())) {
                    clickHouseAdapter.deleteConfig(file.getName());

                    logger.info("Delete a clickhouse mapping config: {} of canal adapter", file.getName());
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }
}
