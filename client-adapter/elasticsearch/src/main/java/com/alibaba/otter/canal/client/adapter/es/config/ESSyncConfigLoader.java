package com.alibaba.otter.canal.client.adapter.es.config;

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

public class ESSyncConfigLoader {

    private static Logger       logger    = LoggerFactory.getLogger(ESSyncConfigLoader.class);

    private static final String BASE_PATH = "es";

    public static Map<String, ESSyncConfig> load() {
        logger.info("## Start loading mapping config ... ");

        Map<String, ESSyncConfig> result = new LinkedHashMap<>();

        Collection<String> configs = AdapterConfigs.get("es");
        for (String c : configs) {
            if (c == null) {
                continue;
            }
            c = c.trim();
            if (c.equals("") || c.startsWith("#")) {
                continue;
            }

            ESSyncConfig config;
            String configContent = null;

            if (c.endsWith(".yml")) {
                configContent = readConfigContent(BASE_PATH + "/" + c);
            }

            config = new Yaml().loadAs(configContent, ESSyncConfig.class);

            try {
                config.validate();
            } catch (Exception e) {
                throw new RuntimeException("ERROR Config: " + c + " " + e.getMessage(), e);
            }
            result.put(c, config);
        }

        logger.info("## Mapping config loaded");
        return result;
    }

    public static String readConfigContent(String config) {
        InputStream in = null;
        try {
            // 先取本地文件，再取类路径
            File configFile = new File("config/" + config);
            if (configFile.exists()) {
                in = new FileInputStream(configFile);
            } else {
                in = ESSyncConfigLoader.class.getClassLoader().getResourceAsStream(config);
            }
            if (in == null) {
                throw new RuntimeException("Config file not found.");
            }

            byte[] bytes = new byte[in.available()];
            in.read(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Read yml config error ", e);
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
