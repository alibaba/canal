package com.alibaba.otter.canal.client.adapter.support;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.lang.StringUtils;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.origin.OriginTrackedValue;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

public class YamlUtils {

    public static <T> T resourceYmlToObj(String resource, String prefix, Class<T> clazz) {
        ClassPathResource classPathResource = new ClassPathResource(resource);

        String content;
        try (InputStream inputStream = classPathResource.getInputStream()) {

            ByteArrayOutputStream result = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int length;
            while ((length = inputStream.read(buffer)) != -1) {
                result.write(buffer, 0, length);
            }
            content = result.toString("UTF-8");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return ymlToObj(prefix, content, clazz);
    }

    public static <T> T ymlToObj(String content, Class<T> clazz) {
        return ymlToObj("", content, clazz, null, null);
    }

    public static <T> T ymlToObj(String prefix, String content, Class<T> clazz) {
        return ymlToObj(prefix, content, clazz, null, null);
    }

    public static <T> T ymlToObj(String prefix, String content, Class<T> clazz, String charset) {
        return ymlToObj(prefix, content, clazz, charset, null);
    }

    public static <T> T ymlToObj(String prefix, String content, Class<T> clazz, String charset,
                                 Properties baseProperties) {
        try {
            prefix = StringUtils.trimToEmpty(prefix);
            byte[] contentBytes;
            if (charset == null) {
                contentBytes = content.getBytes(StandardCharsets.UTF_8);
            } else {
                contentBytes = content.getBytes(charset);
            }
            YamlPropertySourceLoader propertySourceLoader = new YamlPropertySourceLoader();
            Resource configResource = new ByteArrayResource(contentBytes);
            List<PropertySource<?>> propertySources = propertySourceLoader.load("manualBindConfig", configResource);

            if (propertySources == null || propertySources.isEmpty()) {
                return null;
            }

            PropertySource<?> propertySource = propertySources.get(0);

            Properties properties = new Properties();
            if (baseProperties != null) {
                properties.putAll(baseProperties);
            }

            properties.putAll((Map<?, ?>) propertySource.getSource());

            for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                Object value = entry.getValue();
                if (value instanceof OriginTrackedValue) {
                    entry.setValue(((OriginTrackedValue) value).getValue());
                }
            }
            
            ConfigurationPropertySource sources = new MapConfigurationPropertySource(properties);
            Binder binder = new Binder(sources);
            return binder.bind(prefix, Bindable.of(clazz)).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
