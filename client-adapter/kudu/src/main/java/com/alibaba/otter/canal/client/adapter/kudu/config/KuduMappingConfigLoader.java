package com.alibaba.otter.canal.client.adapter.kudu.config;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.adapter.config.YmlConfigBinder;
import com.alibaba.otter.canal.client.adapter.support.MappingConfigsLoader;

/**
 * @author liuyadong
 * @description kudu表信息加载
 */
public class KuduMappingConfigLoader {

    private static Logger logger = LoggerFactory.getLogger(KuduMappingConfigLoader.class);

    /**
     * 加载HBase表映射配置
     *
     * @return 配置名/配置文件名--对象
     */
    public static Map<String, KuduMappingConfig> load(Properties envProperties) {
        logger.info("## Start loading kudu mapping config ... ");

        Map<String, KuduMappingConfig> result = new LinkedHashMap<>();

        Map<String, String> configContentMap = MappingConfigsLoader.loadConfigs("kudu");
        configContentMap.forEach((fileName, content) -> {
            KuduMappingConfig config = YmlConfigBinder.bindYmlToObj(null,
                content,
                KuduMappingConfig.class,
                null,
                envProperties);
            if (config == null) {
                return;
            }
            try {
                config.validate();
            } catch (Exception e) {
                throw new RuntimeException("ERROR load Config: " + fileName + " " + e.getMessage(), e);
            }
            result.put(fileName, config);
        });

        logger.info("## kudu mapping config loaded");
        return result;
    }
}
