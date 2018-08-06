package com.alibaba.otter.canal.common.utils;

/**
 * 用于dump记录counter
 * 这种场景下，
 * <strong>写操作>>读操作</strong>
 * dump线程为串行，读取counter时，并不严格要求最新值可见，使用普通long即可。
 *
 * @author Chuanyi Li
 */
public class SerializedLongAdder {

    private long value = 0;

    public SerializedLongAdder(long initial) {
        this.value = initial;
    }

    public void add(long x) {
        value += x;
    }

    public void increment() {
        value ++;
    }

    public long get() {
        return value;
    }

}
