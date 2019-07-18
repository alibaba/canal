package com.alibaba.otter.canal.client.adapter.es.config;

import com.alibaba.otter.canal.client.adapter.config.YmlConfigBinder;
import com.alibaba.otter.canal.client.adapter.support.MappingConfigsLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * ES 配置装载器
 *
 * @author rewerma 2018-11-01
 * @version 1.0.0
 */
public class ESSyncConfigLoader {

    private static Logger logger = LoggerFactory.getLogger(ESSyncConfigLoader.class);

    public static synchronized Map<String, ESSyncConfig> load(Properties envProperties) {
        logger.info("## Start loading es mapping config ... ");

        Map<String, ESSyncConfig> esSyncConfig = new LinkedHashMap<>();

        Map<String, String> configContentMap = MappingConfigsLoader.loadConfigs(envProperties.getProperty("esType"));
        configContentMap.forEach((fileName, content) -> {
            ESSyncConfig config = YmlConfigBinder.bindYmlToObj(null, content, ESSyncConfig.class, null, envProperties);
            if (config == null) {
                return;
            }
            try {
                config.validate();
            } catch (Exception e) {
                throw new RuntimeException("ERROR Config: " + fileName + " " + e.getMessage(), e);
            }
            esSyncConfig.put(fileName, config);
        });

        logger.info("## ES mapping config loaded");
        return esSyncConfig;
    }
}
