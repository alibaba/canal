package com.alibaba.otter.canal.client.adapter.rdb.config;

import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.alibaba.otter.canal.client.adapter.support.MappingConfigsLoader;

/**
 * RDB表映射配置加载器
 *
 * @author rewerma 2018-11-07 下午02:41:34
 * @version 1.0.0
 */
public class ConfigLoader {

    private static Logger logger = LoggerFactory.getLogger(ConfigLoader.class);

    /**
     * 加载HBase表映射配置
     * 
     * @return 配置名/配置文件名--对象
     */
    public static Map<String, MappingConfig> load() {
        logger.info("## Start loading rdb mapping config ... ");

        Map<String, MappingConfig> result = new LinkedHashMap<>();

        Map<String, String> configContentMap = MappingConfigsLoader.loadConfigs("rdb");
        configContentMap.forEach((fileName, content) -> {
            MappingConfig config = new Yaml().loadAs(content, MappingConfig.class);
            try {
                config.validate();
            } catch (Exception e) {
                throw new RuntimeException("ERROR Config: " + fileName + " " + e.getMessage(), e);
            }
            result.put(fileName, config);
        });

        logger.info("## Rdb mapping config loaded");
        return result;
    }
}
