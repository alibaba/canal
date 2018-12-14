package com.alibaba.otter.canal.client.adapter.hbase.config;

import java.util.LinkedHashMap;
import java.util.Map;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.alibaba.otter.canal.client.adapter.support.MappingConfigsLoader;

/**
 * HBase表映射配置加载器
 *
 * @author rewerma 2018-8-21 下午06:45:49
 * @version 1.0.0
 */
public class MappingConfigLoader {

    private static Logger logger = LoggerFactory.getLogger(MappingConfigLoader.class);

    /**
     * 加载HBase表映射配置
     *
     * @return 配置名/配置文件名--对象
     */
    @SuppressWarnings("unchecked")
    public static Map<String, MappingConfig> load() {
        logger.info("## Start loading hbase mapping config ... ");

        Map<String, MappingConfig> result = new LinkedHashMap<>();

        Map<String, String> configContentMap = MappingConfigsLoader.loadConfigs("hbase");
        configContentMap.forEach((fileName, content) -> {
            Map configMap = new Yaml().loadAs(content, Map.class); // yml自带的对象反射不是很稳定
            JSONObject configJson = new JSONObject(configMap);
            MappingConfig config = configJson.toJavaObject(MappingConfig.class);
            try {
                config.validate();
            } catch (Exception e) {
                throw new RuntimeException("ERROR load Config: " + fileName + " " + e.getMessage(), e);
            }
            result.put(fileName, config);
        });

        logger.info("## Hbase mapping config loaded");
        return result;
    }
}
