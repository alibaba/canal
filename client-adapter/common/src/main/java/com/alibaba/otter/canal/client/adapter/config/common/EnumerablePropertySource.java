package com.alibaba.otter.canal.client.adapter.config.common;

import org.springframework.util.ObjectUtils;

/**
 * A {@link PropertySource} implementation capable of interrogating its
 * underlying source object to enumerate all possible property name/value pairs.
 * Exposes the {@link #getPropertyNames()} method to allow callers to introspect
 * available properties without having to access the underlying source object.
 * This also facilitates a more efficient implementation of
 * {@link #containsProperty(String)}, in that it can call
 * {@link #getPropertyNames()} and iterate through the returned array rather
 * than attempting a call to {@link #getProperty(String)} which may be more
 * expensive. Implementations may consider caching the result of
 * {@link #getPropertyNames()} to fully exploit this performance opportunity.
 * <p>
 * Most framework-provided {@code PropertySource} implementations are
 * enumerable; a counter-example would be {@code JndiPropertySource} where, due
 * to the nature of JNDI it is not possible to determine all possible property
 * names at any given time; rather it is only possible to try to access a
 * property (via {@link #getProperty(String)}) in order to evaluate whether it
 * is present or not.
 *
 * @author Chris Beams
 * @author Juergen Hoeller
 * @since 3.1
 */
public abstract class EnumerablePropertySource<T> extends PropertySource<T> {

    public EnumerablePropertySource(String name, T source){
        super(name, source);
    }

    protected EnumerablePropertySource(String name){
        super(name);
    }

    /**
     * Return whether this {@code PropertySource} contains a property with the given
     * name.
     * <p>
     * This implementation checks for the presence of the given name within the
     * {@link #getPropertyNames()} array.
     *
     * @param name the name of the property to find
     */
    @Override
    public boolean containsProperty(String name) {
        return ObjectUtils.containsElement(getPropertyNames(), name);
    }

    /**
     * Return the names of all properties contained by the {@linkplain #getSource()
     * source} object (never {@code null}).
     */
    public abstract String[] getPropertyNames();

}
