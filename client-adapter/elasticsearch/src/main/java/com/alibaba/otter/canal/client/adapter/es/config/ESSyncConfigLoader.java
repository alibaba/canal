package com.alibaba.otter.canal.client.adapter.es.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.client.adapter.support.AdapterConfigs;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;

/**
 * ES 配置装载器
 *
 * @author rewerma 2018-11-01
 * @version 1.0.0
 */
public class ESSyncConfigLoader {

    private static Logger                                   logger              = LoggerFactory
        .getLogger(ESSyncConfigLoader.class);

    public static synchronized Map<String, ESSyncConfig> load(String name) {
        logger.info("## Start loading es mapping config ... ");

        Map<String, ESSyncConfig> esSyncConfig = new LinkedHashMap<>();

        Collection<String> configs = AdapterConfigs.get("es");
        if (configs == null) {
            return esSyncConfig;
        }
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
                configContent = readConfigContent(name + "/" + c);
            }

            config = new Yaml().loadAs(configContent, ESSyncConfig.class);

            try {
                config.validate();
            } catch (Exception e) {
                throw new RuntimeException("ERROR Config: " + c, e);
            }
            esSyncConfig.put(c, config);
        }

        logger.info("## ES mapping config loaded");
        return esSyncConfig;
    }

    private static String readConfigContent(String config) {
        InputStream in = null;
        try {
            // 先取本地文件，再取类路径
            File configFile = new File("../conf/" + config);
            if (configFile.exists()) {
                in = new FileInputStream(configFile);
            } else {
                    in = ESSyncConfigLoader.class.getClassLoader().getResourceAsStream(config);
            }
            if (in == null) {
                throw new RuntimeException("Config file: " + config + " not found.");
            }

            byte[] bytes = new byte[in.available()];
            in.read(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Read es mapping config error ", e);
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
