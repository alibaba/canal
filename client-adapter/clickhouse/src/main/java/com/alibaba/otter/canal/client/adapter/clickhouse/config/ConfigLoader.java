package com.alibaba.otter.canal.client.adapter.clickhouse.config;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.adapter.support.MappingConfigsLoader;
import com.alibaba.otter.canal.client.adapter.support.YamlUtils;

/**
 * CLICKHOUSE表映射配置加载器
 *
 * @author: Xander
 * @date: Created in 2023/11/10 22:23
 * @email: zhrunxin33@gmail.com
 * @version 1.1.8
 */
public class ConfigLoader {

    private static Logger logger = LoggerFactory.getLogger(ConfigLoader.class);

    /**
     * 加载CLICKHOUSE表映射配置
     *
     * @return 配置名/配置文件名--对象
     */
    public static Map<String, MappingConfig> load(Properties envProperties) {
        logger.info("## Start loading clickhouse mapping config ... ");

        Map<String, MappingConfig> result = new LinkedHashMap<>();

        Map<String, String> configContentMap = MappingConfigsLoader.loadConfigs("clickhouse");
        configContentMap.forEach((fileName, content) -> {
            MappingConfig config = YamlUtils.ymlToObj(null, content, MappingConfig.class, null, envProperties);
            if (config == null) {
                return;
            }
            try {
                config.validate();
            } catch (Exception e) {
                throw new RuntimeException("ERROR Config: " + fileName + " " + e.getMessage(), e);
            }
            result.put(fileName, config);
        });

        logger.info("## ClickHouse mapping config loaded");
        return result;
    }
}
