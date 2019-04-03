package com.alibaba.otter.canal.client.adapter.config.common;

import java.util.Map;
import java.util.Properties;

import org.springframework.core.env.PropertySource;

/**
 * {@link PropertySource} implementation that extracts properties from a
 * {@link Properties} object.
 * <p>
 * Note that because a {@code Properties} object is technically an
 * {@code <Object, Object>} {@link java.util.Hashtable Hashtable}, one may
 * contain non-{@code String} keys or values. This implementation, however is
 * restricted to accessing only {@code String}-based keys and values, in the
 * same fashion as {@link Properties#getProperty} and
 * {@link Properties#setProperty}.
 *
 * @author Chris Beams
 * @author Juergen Hoeller
 * @since 3.1
 */
public class PropertiesPropertySource extends MapPropertySource {

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public PropertiesPropertySource(String name, Properties source){
        super(name, (Map) source);
    }

    protected PropertiesPropertySource(String name, Map<String, Object> source){
        super(name, source);
    }

}
