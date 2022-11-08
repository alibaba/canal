package com.google.common.collect;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Function;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class MigrateMap {

    public static <K, V> ConcurrentMap<K, V> makeComputingMap(CacheBuilder<Object, Object> builder,
                                                              Function<? super K, ? extends V> computingFunction) {
        final Function<? super K, ? extends V> function = computingFunction;
        LoadingCache<K, V> computingCache = builder.build(new CacheLoader<K, V>() {

            @Override
            public V load(K key) throws Exception {
                return function.apply(key);
            }
        });

        return new MigrateConcurrentMap<>(computingCache);
    }

    public static <K, V> ConcurrentMap<K, V> makeComputingMap(Function<? super K, ? extends V> computingFunction) {
        return makeComputingMap(CacheBuilder.newBuilder(), computingFunction);
    }

    final static class MigrateConcurrentMap<K, V> implements ConcurrentMap<K, V> {

        private final LoadingCache<K, V>  computingCache;

        private final ConcurrentMap<K, V> cacheView;

        MigrateConcurrentMap(LoadingCache<K, V> computingCache){
            this.computingCache = computingCache;
            this.cacheView = computingCache.asMap();
        }

        @Override
        public int size() {
            return cacheView.size();
        }

        @Override
        public boolean isEmpty() {
            return cacheView.isEmpty();
        }

        @Override
        public boolean containsKey(Object key) {
            return cacheView.containsKey(key);
        }

        @Override
        public boolean containsValue(Object value) {
            return cacheView.containsValue(value);
        }

        @Override
        public V get(Object key) {
            try {
                return computingCache.get((K) key);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public V put(K key, V value) {
            return cacheView.put(key, value);
        }

        @Override
        public V remove(Object key) {
            return cacheView.remove(key);
        }

        @Override
        public void putAll(Map<? extends K, ? extends V> m) {
            cacheView.putAll(m);
        }

        @Override
        public void clear() {
            cacheView.clear();
        }

        @Override
        public Set<K> keySet() {
            return cacheView.keySet();
        }

        @Override
        public Collection<V> values() {
            return cacheView.values();
        }

        @Override
        public Set<Entry<K, V>> entrySet() {
            return cacheView.entrySet();
        }

        @Override
        public V putIfAbsent(K key, V value) {
            return cacheView.putIfAbsent(key, value);
        }

        @Override
        public boolean remove(Object key, Object value) {
            return cacheView.remove(key, value);
        }

        @Override
        public boolean replace(K key, V oldValue, V newValue) {
            return cacheView.replace(key, oldValue, newValue);
        }

        @Override
        public V replace(K key, V value) {
            return cacheView.replace(key, value);
        }

        @Override
        public String toString() {
            return cacheView.toString();
        }
    }
}
