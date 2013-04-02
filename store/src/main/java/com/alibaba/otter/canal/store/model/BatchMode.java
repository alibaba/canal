package com.alibaba.otter.canal.store.model;

/**
 * 批处理模式
 * 
 * @author jianghang 2013-3-18 上午11:51:15
 * @version 1.0.3
 */
public enum BatchMode {

    /** 对象数量 */
    ITEMSIZE,

    /** 内存大小 */
    MEMSIZE;

    public boolean isItemSize() {
        return this == BatchMode.ITEMSIZE;
    }

    public boolean isMemSize() {
        return this == BatchMode.MEMSIZE;
    }
}
