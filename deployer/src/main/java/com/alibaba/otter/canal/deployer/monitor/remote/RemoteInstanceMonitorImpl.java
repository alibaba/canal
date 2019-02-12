package com.alibaba.otter.canal.deployer.monitor.remote;

import java.io.File;
import java.io.FileWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.common.utils.CommonUtils;

/**
 * 远程xxx/instance.properties配置监听器实现
 *
 * @author rewerma 2019-01-25 下午05:20:16
 * @version 1.0.0
 */
public class RemoteInstanceMonitorImpl implements RemoteInstanceMonitor {

    private static final Logger logger = LoggerFactory.getLogger(RemoteInstanceMonitorImpl.class);

    @Override
    public void onAdd(ConfigItem configItem) {
        this.onModify(configItem);
    }

    @Override
    public void onModify(ConfigItem configItem) {
        String confDir = CommonUtils.getConfPath() + configItem.getName();
        File instanceDir = new File(confDir);
        if (!instanceDir.exists()) {
            boolean mkDirs = instanceDir.mkdirs();
            if (!mkDirs) {
                logger.info("## Error to create instance config dir: {}", configItem.getName());
                return;
            }
        }
        try (FileWriter writer = new FileWriter(confDir + "/instance.properties")) {
            writer.write(configItem.getContent());
            writer.flush();
            logger.info("## Loaded remote instance config: {}/instance.properties ", configItem.getName());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void onDelete(String instanceName) {
        File file = new File(CommonUtils.getConfPath() + instanceName + "/");
        if (file.exists()) {
            CommonUtils.deleteDir(file);
            logger.info("## Deleted and loaded remote instance config: {} ", instanceName);
        }
    }

}
