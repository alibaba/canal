package com.alibaba.otter.canal.client.adapter.support;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.NoSuchElementException;

public class URLClassExtensionLoader extends URLClassLoader {

    public URLClassExtensionLoader(URL[] urls){
        super(urls);
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        Class<?> c = findLoadedClass(name);
        if (c != null) {
            return c;
        }

        if (name.startsWith("java.") || name.startsWith("org.slf4j.") || name.startsWith("org.apache.logging")
            || name.startsWith("org.apache.zookeeper.") || name.startsWith("org.I0Itec.zkclient.")
            || name.startsWith("org.apache.commons.logging.")) {
            // || name.startsWith("org.apache.hadoop."))
            // {
            c = super.loadClass(name);
        }
        if (c != null) return c;

        try {
            // 先加载jar内的class，可避免jar冲突
            c = findClass(name);
        } catch (ClassNotFoundException e) {
            c = null;
        }
        if (c != null) {
            return c;
        }

        return super.loadClass(name);
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        @SuppressWarnings("unchecked")
        Enumeration<URL>[] tmp = (Enumeration<URL>[]) new Enumeration<?>[2];

        tmp[0] = findResources(name); // local class
        // path first
        // tmp[1] = super.getResources(name);

        return new CompoundEnumeration<>(tmp);
    }

    private static class CompoundEnumeration<E> implements Enumeration<E> {

        private Enumeration<E>[] enums;
        private int              index = 0;

        public CompoundEnumeration(Enumeration<E>[] enums){
            this.enums = enums;
        }

        private boolean next() {
            while (this.index < this.enums.length) {
                if (this.enums[this.index] != null && this.enums[this.index].hasMoreElements()) {
                    return true;
                }

                ++this.index;
            }

            return false;
        }

        public boolean hasMoreElements() {
            return this.next();
        }

        public E nextElement() {
            if (!this.next()) {
                throw new NoSuchElementException();
            } else {
                return this.enums[this.index].nextElement();
            }
        }
    }
}
