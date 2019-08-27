package com.alibaba.otter.canal.instance.spring.support;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ResourceLoaderAware;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.util.Assert;

/**
 * 扩展Spring的
 * {@linkplain org.springframework.beans.factory.config.PropertyPlaceholderConfigurer}
 * ，增加默认值的功能。 例如：${placeholder:defaultValue}，假如placeholder的值不存在，则默认取得
 * defaultValue。
 * 
 * @author jianghang 2013-1-24 下午03:37:56
 * @version 1.0.0
 */
public class PropertyPlaceholderConfigurer extends org.springframework.beans.factory.config.PropertyPlaceholderConfigurer implements ResourceLoaderAware, InitializingBean {

    private static final String           PLACEHOLDER_PREFIX = "${";
    private static final String           PLACEHOLDER_SUFFIX = "}";
    public static ThreadLocal<Properties> propertiesLocal    = new ThreadLocal<Properties>() {

                                                                 @Override
                                                                 protected Properties initialValue() {
                                                                     return new Properties();
                                                                 }

                                                             };

    private ResourceLoader                loader;
    private String[]                      locationNames;

    public PropertyPlaceholderConfigurer(){
        setIgnoreUnresolvablePlaceholders(true);
    }

    public void setResourceLoader(ResourceLoader loader) {
        this.loader = loader;
    }

    public void setLocationNames(String[] locations) {
        this.locationNames = locations;
    }

    public void afterPropertiesSet() throws Exception {
        Assert.notNull(loader, "no resourceLoader");

        if (locationNames != null) {
            for (int i = 0; i < locationNames.length; i++) {
                locationNames[i] = resolveSystemPropertyPlaceholders(locationNames[i]);
            }
        }

        if (locationNames != null) {
            List<Resource> resources = new ArrayList<Resource>(locationNames.length);

            for (String location : locationNames) {
                location = trimToNull(location);

                if (location != null) {
                    resources.add(loader.getResource(location));
                }
            }

            super.setLocations(resources.toArray(new Resource[resources.size()]));
        }
    }

    private String resolveSystemPropertyPlaceholders(String text) {
        StringBuilder buf = new StringBuilder(text);

        for (int startIndex = buf.indexOf(PLACEHOLDER_PREFIX); startIndex >= 0;) {
            int endIndex = buf.indexOf(PLACEHOLDER_SUFFIX, startIndex + PLACEHOLDER_PREFIX.length());

            if (endIndex != -1) {
                String placeholder = buf.substring(startIndex + PLACEHOLDER_PREFIX.length(), endIndex);
                int nextIndex = endIndex + PLACEHOLDER_SUFFIX.length();

                try {
                    String value = resolveSystemPropertyPlaceholder(placeholder);

                    if (value != null) {
                        buf.replace(startIndex, endIndex + PLACEHOLDER_SUFFIX.length(), value);
                        nextIndex = startIndex + value.length();
                    } else {
                        System.err.println("Could not resolve placeholder '"
                                           + placeholder
                                           + "' in ["
                                           + text
                                           + "] as system property: neither system property nor environment variable found");
                    }
                } catch (Throwable ex) {
                    System.err.println("Could not resolve placeholder '" + placeholder + "' in [" + text
                                       + "] as system property: " + ex);
                }

                startIndex = buf.indexOf(PLACEHOLDER_PREFIX, nextIndex);
            } else {
                startIndex = -1;
            }
        }

        return buf.toString();
    }

    private String resolveSystemPropertyPlaceholder(String placeholder) {
        DefaultablePlaceholder dp = new DefaultablePlaceholder(placeholder);
        String value = System.getProperty(dp.placeholder);

        if (value == null) {
            value = System.getenv(dp.placeholder);
        }

        if (value == null) {
            value = dp.defaultValue;
        }

        return value;
    }

    @Override
    protected String resolvePlaceholder(String placeholder, Properties props, int systemPropertiesMode) {
        DefaultablePlaceholder dp = new DefaultablePlaceholder(placeholder);
        String propVal = null;
        // 以system为准覆盖本地配置, 适用于docker
        if (systemPropertiesMode == SYSTEM_PROPERTIES_MODE_OVERRIDE) {
            propVal = resolveSystemProperty(dp.placeholder);
        }

        // 以threadlocal的为准覆盖file properties
        if (propVal == null) {
            Properties localProperties = propertiesLocal.get();
            propVal = resolvePlaceholder(dp.placeholder, localProperties);
        }

        if (propVal == null) {
            propVal = resolvePlaceholder(dp.placeholder, props);
        }

        if (propVal == null && systemPropertiesMode == SYSTEM_PROPERTIES_MODE_FALLBACK) {
            propVal = resolveSystemProperty(dp.placeholder);
        }

        if (propVal == null) {
            propVal = dp.defaultValue;
        }

        return trimToEmpty(propVal);
    }

    private static class DefaultablePlaceholder {

        private final String defaultValue;
        private final String placeholder;

        public DefaultablePlaceholder(String placeholder){
            int commaIndex = placeholder.indexOf(":");
            String defaultValue = null;

            if (commaIndex >= 0) {
                defaultValue = trimToEmpty(placeholder.substring(commaIndex + 1));
                placeholder = trimToEmpty(placeholder.substring(0, commaIndex));
            }

            this.placeholder = placeholder;
            this.defaultValue = defaultValue;
        }
    }

    private String trimToNull(String str) {
        if (str == null) {
            return null;
        }

        String result = str.trim();

        if (result == null || result.length() == 0) {
            return null;
        }

        return result;
    }

    public static String trimToEmpty(String str) {
        if (str == null) {
            return "";
        }

        return str.trim();
    }
}
