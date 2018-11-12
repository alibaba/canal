package com.alibaba.otter.canal.client.adapter.es.config;

import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.alibaba.otter.canal.client.adapter.support.MappingConfigsLoader;

/**
 * ES 配置装载器
 *
 * @author rewerma 2018-11-01
 * @version 1.0.0
 */
public class ESSyncConfigLoader {

    private static Logger logger = LoggerFactory.getLogger(ESSyncConfigLoader.class);

    public static synchronized Map<String, ESSyncConfig> load() {
        logger.info("## Start loading es mapping config ... ");

        Map<String, ESSyncConfig> esSyncConfig = new LinkedHashMap<>();

        Map<String, String> configContentMap = MappingConfigsLoader.loadConfigs("es");
        configContentMap.forEach((fileName, content) -> {
            ESSyncConfig config = new Yaml().loadAs(content, ESSyncConfig.class);
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
