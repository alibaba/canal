package com.alibaba.otter.canal.client.adapter.es.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
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

    private static final String                             BASE_PATH           = "es";

    private static volatile Map<String, ESSyncConfig>       esSyncConfig        = new LinkedHashMap<>(); // 文件名对应配置
    private static volatile Map<String, List<ESSyncConfig>> dbTableEsSyncConfig = new LinkedHashMap<>(); // schema-table对应配置

    public static Map<String, ESSyncConfig> getEsSyncConfig() {
        return esSyncConfig;
    }

    public static Map<String, List<ESSyncConfig>> getDbTableEsSyncConfig() {
        return dbTableEsSyncConfig;
    }

    public static synchronized void load() {
        logger.info("## Start loading mapping config ... ");
        Collection<String> configs = AdapterConfigs.get("es");
        if (configs == null) {
            return;
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
                configContent = readConfigContent(BASE_PATH + "/" + c);
            }

            config = new Yaml().loadAs(configContent, ESSyncConfig.class);

            try {
                config.validate();
                SchemaItem schemaItem = SqlParser.parse(config.getEsMapping().getSql());
                config.getEsMapping().setSchemaItem(schemaItem);

                DruidDataSource dataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
                if (dataSource == null || dataSource.getUrl() == null) {
                    throw new RuntimeException("No data source found: " + config.getDataSourceKey());
                }
                Pattern pattern = Pattern.compile(".*:(.*)://.*/(.*)\\?.*$");
                Matcher matcher = pattern.matcher(dataSource.getUrl());
                if (!matcher.find()) {
                    throw new RuntimeException("Not found the schema of jdbc-url: " + config.getDataSourceKey());
                }
                String schema = matcher.group(2);

                schemaItem.getAliasTableItems().values().forEach(tableItem -> {
                    List<ESSyncConfig> esSyncConfigs = dbTableEsSyncConfig
                        .computeIfAbsent(schema + "-" + tableItem.getTableName(), k -> new ArrayList<>());
                    esSyncConfigs.add(config);
                });
            } catch (Exception e) {
                throw new RuntimeException("ERROR Config: " + c, e);
            }
            esSyncConfig.put(c, config);
        }

        logger.info("## Mapping config loaded");
    }

    private static String readConfigContent(String config) {
        InputStream in = null;
        try {
            // 先取本地文件，再取类路径
            File configFile = new File("../config/" + config);
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
