package com.alibaba.otter.canal.client.adapter.support;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class MappingConfigsLoader {

    public static Map<String, String> loadConfigs(String name) {
        Map<String, String> configContentMap = new HashMap<>();

        // 先取本地文件，再取类路径
        File configDir = new File(".." + File.separator + Constant.CONF_DIR + File.separator + name);
        if (!configDir.exists()) {
            URL url = MappingConfigsLoader.class.getClassLoader().getResource("");
            if (url != null) {
                configDir = new File(url.getPath() + name + File.separator);
            }
        }

        File[] files = configDir.listFiles();
        if (files != null) {
            for (File file : files) {
                String fileName = file.getName();
                if (!fileName.endsWith(".yml")) {
                    continue;
                }
                try (InputStream in = new FileInputStream(file)) {
                    byte[] bytes = new byte[in.available()];
                    in.read(bytes);
                    String configContent = new String(bytes, StandardCharsets.UTF_8);
                    configContentMap.put(fileName, configContent);
                } catch (IOException e) {
                    throw new RuntimeException("Read " + name + "mapping config: " + fileName + " error. ", e);
                }
            }
        }

        return configContentMap;
    }

    public static String loadConfig(String name) {
        // 先取本地文件，再取类路径
        File filePath = new File(".." + File.separator + Constant.CONF_DIR + File.separator + name);
        if (!filePath.exists()) {
            URL url = MappingConfigsLoader.class.getClassLoader().getResource("");
            if (url != null) {
                filePath = new File(url.getPath() + name);
            }
        }
        if (filePath.exists()) {
            String fileName = filePath.getName();
            if (!fileName.endsWith(".yml")) {
                return null;
            }
            try (InputStream in = new FileInputStream(filePath)) {
                byte[] bytes = new byte[in.available()];
                in.read(bytes);
                return new String(bytes, StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new RuntimeException("Read mapping config: " + filePath.getAbsolutePath() + " error. ", e);
            }
        }
        return null;
    }
}
