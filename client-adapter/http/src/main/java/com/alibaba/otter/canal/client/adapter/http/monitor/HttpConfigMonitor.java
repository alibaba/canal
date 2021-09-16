/**
 * Created by Wu Jian Ping on - 2021/09/15.
 */

package com.alibaba.otter.canal.client.adapter.http.monitor;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.adapter.config.YmlConfigBinder;
import com.alibaba.otter.canal.client.adapter.http.HttpAdapter;
import com.alibaba.otter.canal.client.adapter.http.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.support.MappingConfigsLoader;
import com.alibaba.otter.canal.client.adapter.support.Util;

public class HttpConfigMonitor {

    private static final Logger logger = LoggerFactory.getLogger(HttpConfigMonitor.class);

    private static final String adapterName = "http";

    private HttpAdapter httpAdapter;

    private Properties envProperties;

    private FileAlterationMonitor fileMonitor;

    public void init(HttpAdapter httpAdapter, Properties envProperties) {
        this.httpAdapter = httpAdapter;
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
                MappingConfig config = YmlConfigBinder.bindYmlToObj(null, configContent, MappingConfig.class, null,
                        envProperties);
                if (config == null) {
                    return;
                }
                config.validate();
                addConfigToCache(file, config);

                logger.info("Add a new http mapping config: {} to canal adapter", file.getName());
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        @Override
        public void onFileChange(File file) {
            super.onFileChange(file);

            try {
                if (httpAdapter.getHttpMapping().containsKey(file.getName())) {
                    // 加载配置文件
                    String configContent = MappingConfigsLoader
                            .loadConfig(adapterName + File.separator + file.getName());
                    if (configContent == null) {
                        onFileDelete(file);
                        return;
                    }
                    MappingConfig config = YmlConfigBinder.bindYmlToObj(null, configContent, MappingConfig.class, null,
                            envProperties);
                    if (config == null) {
                        return;
                    }
                    config.validate();
                    if (httpAdapter.getHttpMapping().containsKey(file.getName())) {
                        deleteConfigFromCache(file);
                    }
                    addConfigToCache(file, config);

                    logger.info("Update a http mapping config: {} of canal adapter", file.getName());
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        @Override
        public void onFileDelete(File file) {
            super.onFileDelete(file);

            try {
                if (httpAdapter.getHttpMapping().containsKey(file.getName())) {
                    deleteConfigFromCache(file);

                    logger.info("Delete a http mapping config: {} of canal adapter", file.getName());
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        private void addConfigToCache(File file, MappingConfig config) {
            httpAdapter.getHttpMapping().put(file.getName(), config);
            Map<String, MappingConfig> configMap = httpAdapter.getMappingConfigCache()
                    .computeIfAbsent(StringUtils.trimToEmpty(config.getDestination()), k1 -> new HashMap<>());
            configMap.put(file.getName(), config);
        }

        private void deleteConfigFromCache(File file) {
            httpAdapter.getHttpMapping().remove(file.getName());
            for (Map<String, MappingConfig> configMap : httpAdapter.getMappingConfigCache().values()) {
                if (configMap != null) {
                    configMap.remove(file.getName());
                }
            }

        }
    }
}
