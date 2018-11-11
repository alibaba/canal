package com.alibaba.otter.canal.client.adapter.rdb.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.alibaba.otter.canal.client.adapter.support.AdapterConfigs;

/**
 * RDB表映射配置加载器
 *
 * @author rewerma 2018-11-07 下午02:41:34
 * @version 1.0.0
 */
public class MappingConfigLoader {

    private static Logger       logger    = LoggerFactory.getLogger(MappingConfigLoader.class);

    /**
     * 加载HBase表映射配置
     * 
     * @return 配置名/配置文件名--对象
     */
    public static Map<String, MappingConfig> load(String name) {
        logger.info("## Start loading rdb mapping config ... ");

        Map<String, MappingConfig> result = new LinkedHashMap<>();

        Collection<String> configs = AdapterConfigs.get(name);
        if (configs == null) {
            return result;
        }
        for (String c : configs) {
            if (c == null) {
                continue;
            }
            c = c.trim();
            if (c.equals("") || c.startsWith("#")) {
                continue;
            }

            String configContent = null;

            if (c.endsWith(".yml")) {
                configContent = readConfigContent(name + "/" + c);
            }

            MappingConfig config = new Yaml().loadAs(configContent, MappingConfig.class);

            try {
                config.validate();
            } catch (Exception e) {
                throw new RuntimeException("ERROR Config: " + c + " " + e.getMessage(), e);
            }
            result.put(c, config);
        }

        logger.info("## Rdb mapping config loaded");
        return result;
    }

    public static String readConfigContent(String config) {
        InputStream in = null;
        try {
            // 先取本地文件，再取类路径
            File configFile = new File("../conf/" + config);
            if (configFile.exists()) {
                in = new FileInputStream(configFile);
            } else {
                in = MappingConfigLoader.class.getClassLoader().getResourceAsStream(config);
            }
            if (in == null) {
                throw new RuntimeException("Rdb mapping config file not found.");
            }

            byte[] bytes = new byte[in.available()];
            in.read(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Read rdb mapping config  error. ", e);
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException e) {
                // ignore
            }
        }
    }
}
