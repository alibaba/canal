package com.alibaba.otter.canal.client.adapter.config.common;

/**
 * Holder containing one or more {@link PropertySource} objects.
 *
 * @author Chris Beams
 * @since 3.1
 */
public interface PropertySources extends Iterable<PropertySource<?>> {

    /**
     * Return whether a property source with the given name is contained.
     *
     * @param name the {@linkplain PropertySource#getName() name of the property source} to find
     */
    boolean contains(String name);

    /**
     * Return the property source with the given name, {@code null} if not found.
     *
     * @param name the {@linkplain PropertySource#getName() name of the property source} to find
     */
    PropertySource<?> get(String name);

}
