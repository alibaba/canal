package com.alibaba.otter.canal.client.adapter.config.bind;

/**
 * Strategy interface used to check if a property name matches specific
 * criteria.
 *
 * @author Phillip Webb
 * @since 1.2.0
 */
interface PropertyNamePatternsMatcher {

    PropertyNamePatternsMatcher ALL  = propertyName -> true;

    PropertyNamePatternsMatcher NONE = propertyName -> false;

    /**
     * Return {@code true} of the property name matches.
     *
     * @param propertyName the property name
     * @return {@code true} if the property name matches
     */
    boolean matches(String propertyName);

}
