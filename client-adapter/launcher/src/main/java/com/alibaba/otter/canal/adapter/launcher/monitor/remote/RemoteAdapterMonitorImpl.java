package com.alibaba.otter.canal.adapter.launcher.monitor.remote;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.common.utils.CommonUtils;

/**
 * 远程配置监听器实现
 *
 * @author rewerma 2019-01-25 下午05:20:16
 * @version 1.0.0
 */
public class RemoteAdapterMonitorImpl implements RemoteAdapterMonitor {

    private static final Logger logger = LoggerFactory.getLogger(RemoteAdapterMonitorImpl.class);

    @Override
    public void onAdd(ConfigItem configItem) {
        this.onModify(configItem);
    }

    @Override
    public void onModify(ConfigItem configItem) {
        String confPath = CommonUtils.getConfPath();
        String category = configItem.getCategory();
        File categoryDir = new File(confPath + category);
        if (!categoryDir.isDirectory()) {
            boolean mkDirs = categoryDir.mkdirs();
            if (!mkDirs) {
                logger.info("## Create adapter category dir error: {}", category);
                return;
            }
        }
        String name = configItem.getName();
        try (OutputStreamWriter writer = new OutputStreamWriter(
            new FileOutputStream(confPath + category + "/" + configItem.getName()),
            StandardCharsets.UTF_8)) {
            writer.write(configItem.getContent());
            writer.flush();
            logger.info("## Loaded remote adapter config: {}/{}", category, name);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void onDelete(String name) {
        File file = new File(CommonUtils.getConfPath() + name);
        if (file.exists()) {
            CommonUtils.deleteDir(file);
            logger.info("## Deleted and reloaded remote adapter config: {}", name);
        }
    }

}
