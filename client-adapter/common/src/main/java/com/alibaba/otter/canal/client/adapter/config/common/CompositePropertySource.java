package com.alibaba.otter.canal.client.adapter.config.common;

import java.util.*;

import org.springframework.util.StringUtils;

/**
 * Composite {@link PropertySource} implementation that iterates over a set of
 * {@link PropertySource} instances. Necessary in cases where multiple property
 * sources share the same name, e.g. when multiple values are supplied to
 * {@code @PropertySource}.
 * <p>
 * As of Spring 4.1.2, this class extends {@link EnumerablePropertySource}
 * instead of plain {@link PropertySource}, exposing {@link #getPropertyNames()}
 * based on the accumulated property names from all contained sources (as far as
 * possible).
 *
 * @author Chris Beams
 * @author Juergen Hoeller
 * @author Phillip Webb
 * @since 3.1.1
 */
public class CompositePropertySource extends EnumerablePropertySource<Object> {

    private final Set<PropertySource<?>> propertySources = new LinkedHashSet<>();

    /**
     * Create a new {@code CompositePropertySource}.
     *
     * @param name the name of the property source
     */
    public CompositePropertySource(String name){
        super(name);
    }

    @Override
    public Object getProperty(String name) {
        for (PropertySource<?> propertySource : this.propertySources) {
            Object candidate = propertySource.getProperty(name);
            if (candidate != null) {
                return candidate;
            }
        }
        return null;
    }

    @Override
    public boolean containsProperty(String name) {
        for (PropertySource<?> propertySource : this.propertySources) {
            if (propertySource.containsProperty(name)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String[] getPropertyNames() {
        Set<String> names = new LinkedHashSet<>();
        for (PropertySource<?> propertySource : this.propertySources) {
            if (!(propertySource instanceof EnumerablePropertySource)) {
                throw new IllegalStateException(
                    "Failed to enumerate property names due to non-enumerable property source: " + propertySource);
            }
            names.addAll(Arrays.asList(((EnumerablePropertySource<?>) propertySource).getPropertyNames()));
        }
        return StringUtils.toStringArray(names);
    }

    /**
     * Add the given {@link PropertySource} to the end of the chain.
     *
     * @param propertySource the PropertySource to add
     */
    public void addPropertySource(PropertySource<?> propertySource) {
        this.propertySources.add(propertySource);
    }

    /**
     * Add the given {@link PropertySource} to the start of the chain.
     *
     * @param propertySource the PropertySource to add
     * @since 4.1
     */
    public void addFirstPropertySource(PropertySource<?> propertySource) {
        List<PropertySource<?>> existing = new ArrayList<>(this.propertySources);
        this.propertySources.clear();
        this.propertySources.add(propertySource);
        this.propertySources.addAll(existing);
    }

    /**
     * Return all property sources that this composite source holds.
     *
     * @since 4.1.1
     */
    public Collection<PropertySource<?>> getPropertySources() {
        return this.propertySources;
    }

    @Override
    public String toString() {
        return String
            .format("%s [name='%s', propertySources=%s]", getClass().getSimpleName(), this.name, this.propertySources);
    }

}
