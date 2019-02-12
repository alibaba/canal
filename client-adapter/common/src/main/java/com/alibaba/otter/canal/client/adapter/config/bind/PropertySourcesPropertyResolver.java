package com.alibaba.otter.canal.client.adapter.config.bind;

import org.springframework.core.convert.ConversionException;
import org.springframework.core.env.AbstractEnvironment;
import org.springframework.core.env.AbstractPropertyResolver;
import org.springframework.core.env.PropertyResolver;
import org.springframework.util.ClassUtils;

import com.alibaba.otter.canal.client.adapter.config.common.PropertySource;
import com.alibaba.otter.canal.client.adapter.config.common.PropertySources;

/**
 * {@link PropertyResolver} implementation that resolves property values against
 * an underlying set of {@link PropertySources}.
 *
 * @author Chris Beams
 * @author Juergen Hoeller
 * @see PropertySource
 * @see PropertySources
 * @see AbstractEnvironment
 * @since 3.1
 */
public class PropertySourcesPropertyResolver extends AbstractPropertyResolver {

    private final PropertySources propertySources;

    /**
     * Create a new resolver against the given property sources.
     *
     * @param propertySources the set of {@link PropertySource} objects to use
     */
    public PropertySourcesPropertyResolver(PropertySources propertySources){
        this.propertySources = propertySources;
    }

    @Override
    public boolean containsProperty(String key) {
        if (this.propertySources != null) {
            for (PropertySource<?> propertySource : this.propertySources) {
                if (propertySource.containsProperty(key)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public String getProperty(String key) {
        return getProperty(key, String.class, true);
    }

    @Override
    public <T> T getProperty(String key, Class<T> targetValueType) {
        return getProperty(key, targetValueType, true);
    }

    @Override
    protected String getPropertyAsRawString(String key) {
        return getProperty(key, String.class, false);
    }

    protected <T> T getProperty(String key, Class<T> targetValueType, boolean resolveNestedPlaceholders) {
        if (this.propertySources != null) {
            for (PropertySource<?> propertySource : this.propertySources) {
                if (logger.isTraceEnabled()) {
                    logger
                        .trace("Searching for key '" + key + "' in PropertySource '" + propertySource.getName() + "'");
                }
                Object value = propertySource.getProperty(key);
                if (value != null) {
                    if (resolveNestedPlaceholders && value instanceof String) {
                        value = resolveNestedPlaceholders((String) value);
                    }
                    logKeyFound(key, propertySource, value);
                    return convertValueIfNecessary(value, targetValueType);
                }
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Could not find key '" + key + "' in any property source");
        }
        return null;
    }

    @Deprecated
    public <T> Class<T> getPropertyAsClass(String key, Class<T> targetValueType) {
        if (this.propertySources != null) {
            for (PropertySource<?> propertySource : this.propertySources) {
                if (logger.isTraceEnabled()) {
                    logger.trace(String.format("Searching for key '%s' in [%s]", key, propertySource.getName()));
                }
                Object value = propertySource.getProperty(key);
                if (value != null) {
                    logKeyFound(key, propertySource, value);
                    Class<?> clazz;
                    if (value instanceof String) {
                        try {
                            clazz = ClassUtils.forName((String) value, null);
                        } catch (Exception ex) {
                            throw new PropertySourcesPropertyResolver.ClassConversionException((String) value,
                                targetValueType,
                                ex);
                        }
                    } else if (value instanceof Class) {
                        clazz = (Class<?>) value;
                    } else {
                        clazz = value.getClass();
                    }
                    if (!targetValueType.isAssignableFrom(clazz)) {
                        throw new PropertySourcesPropertyResolver.ClassConversionException(clazz, targetValueType);
                    }
                    @SuppressWarnings("unchecked")
                    Class<T> targetClass = (Class<T>) clazz;
                    return targetClass;
                }
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Could not find key '%s' in any property source", key));
        }
        return null;
    }

    /**
     * Log the given key as found in the given {@link PropertySource}, resulting in
     * the given value.
     * <p>
     * The default implementation writes a debug log message with key and source. As
     * of 4.3.3, this does not log the value anymore in order to avoid accidental
     * logging of sensitive settings. Subclasses may override this method to change
     * the log level and/or log message, including the property's value if desired.
     *
     * @param key the key found
     * @param propertySource the {@code PropertySource} that the key has been found
     *     in
     * @param value the corresponding value
     * @since 4.3.1
     */
    protected void logKeyFound(String key, PropertySource<?> propertySource, Object value) {
        if (logger.isDebugEnabled()) {
            logger.debug("Found key '" + key + "' in PropertySource '" + propertySource.getName()
                         + "' with value of type " + value.getClass().getSimpleName());
        }
    }

    @SuppressWarnings("serial")
    @Deprecated
    private static class ClassConversionException extends ConversionException {

        public ClassConversionException(Class<?> actual, Class<?> expected){
            super(String
                .format("Actual type %s is not assignable to expected type %s", actual.getName(), expected.getName()));
        }

        public ClassConversionException(String actual, Class<?> expected, Exception ex){
            super(
                String
                    .format("Could not find/load class %s during attempt to convert to %s", actual, expected.getName()),
                ex);
        }
    }

}
