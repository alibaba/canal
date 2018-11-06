package com.alibaba.otter.canal.client.adapter.hbase.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.alibaba.otter.canal.client.adapter.support.AdapterConfigs;

/**
 * HBase表映射配置加载器
 *
 * @author rewerma 2018-8-21 下午06:45:49
 * @version 1.0.0
 */
public class MappingConfigLoader {

    private static Logger       logger    = LoggerFactory.getLogger(MappingConfigLoader.class);

    private static final String BASE_PATH = "hbase";

    /**
     * 加载HBase表映射配置
     * 
     * @return 配置名/配置文件名--对象
     */
    public static Map<String, MappingConfig> load() {
        logger.info("## Start loading mapping config ... ");

        Map<String, MappingConfig> result = new LinkedHashMap<>();

        Collection<String> configs = AdapterConfigs.get("hbase");
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

            MappingConfig config;
            String configContent = null;

            if (c.endsWith(".yml")) {
                configContent = readConfigContent(BASE_PATH + "/" + c);
            }

            // 简单配置database.table@datasourcekey?rowKey=key1,key2
            if (StringUtils.isEmpty(configContent)) {
                String[] mapping = c.split("\\?");
                String params = mapping.length == 2 ? mapping[1] : null;
                String rowKey = null;
                String srcMeta = mapping[0];
                //
                if (params != null) {
                    for (String entry : params.split("&")) {
                        if ("rowKey".equals(entry.split("=")[0])) {
                            rowKey = entry.split("=")[1];
                        }
                    }
                }
                String dsKey = srcMeta.split("@").length == 2 ? srcMeta.split("@")[1] : null;
                String[] dbTable;
                if (dsKey == null) {
                    dbTable = srcMeta.split("\\.");

                } else {
                    dbTable = srcMeta.split("@")[0].split("\\.");
                }

                if (dbTable.length == 2) {
                    config = new MappingConfig();

                    MappingConfig.HbaseMapping hbaseMapping = new MappingConfig.HbaseMapping();
                    hbaseMapping.setHbaseTable(dbTable[0].toUpperCase() + "." + dbTable[1].toUpperCase());
                    hbaseMapping.setAutoCreateTable(true);
                    hbaseMapping.setDatabase(dbTable[0]);
                    hbaseMapping.setTable(dbTable[1]);
                    hbaseMapping.setMode(MappingConfig.Mode.PHOENIX);
                    hbaseMapping.setRowKey(rowKey);
                    // 有定义rowKey
                    if (rowKey != null) {
                        MappingConfig.ColumnItem columnItem = new MappingConfig.ColumnItem();
                        columnItem.setRowKey(true);
                        columnItem.setColumn(rowKey);
                        hbaseMapping.setRowKeyColumn(columnItem);
                    }
                    config.setHbaseMapping(hbaseMapping);
                    config.setDataSourceKey(dsKey);

                } else {
                    throw new RuntimeException(String.format("配置项[%s]内容为空, 或格式不符合database.table", c));
                }

            } else { // 配置文件配置
                config = new Yaml().loadAs(configContent, MappingConfig.class);
            }

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
            File configFile = new File("../config/" + config);
            if (configFile.exists()) {
                in = new FileInputStream(configFile);
            } else {
                in = MappingConfigLoader.class.getClassLoader().getResourceAsStream(config);
            }
            if (in == null) {
                throw new RuntimeException("Config file not found.");
            }

            byte[] bytes = new byte[in.available()];
            in.read(bytes);
            return new String(bytes, "UTF-8");
        } catch (IOException e) {
            throw new RuntimeException("Read ds-config.yml or hbase-mappings.conf error. ", e);
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
