package com.alibaba.otter.canal.admin.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TemplateConfigLoader {

    private static final Logger logger              = LoggerFactory.getLogger(TemplateConfigLoader.class);

    public static final String  CONF_DIR            = "conf";
    public static final String  CANAL_CONFIG_TMP    = "canal-template.properties";
    public static final String  INSTANCE_CONFIG_TMP = "instance-template.properties";

    public static String loadCanalConfig() {
        return loadFile(CANAL_CONFIG_TMP);
    }

    public static String loadInstanceConfig() {
        return loadFile(INSTANCE_CONFIG_TMP);
    }

    private static String loadFile(String fileName) {
        File configFile = new File(".." + File.separator + CONF_DIR + File.separator + fileName);
        if (!configFile.exists()) {
            URL url = TemplateConfigLoader.class.getClassLoader().getResource("");
            if (url != null) {
                configFile = new File(url.getPath() + fileName + File.separator);
            }
        }
        if (!configFile.exists()) {
            return null;
        }
        try (InputStream in = new FileInputStream(configFile)) {
            byte[] bytes = new byte[in.available()];
            in.read(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (IOException e) {
            logger.error("Read " + fileName + " error", e);
        }

        return null;
    }
}
