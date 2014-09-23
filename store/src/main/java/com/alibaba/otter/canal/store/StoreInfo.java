package com.alibaba.otter.canal.store;

/**
 * @author zebin.xuzb 2012-10-30 下午1:05:13
 * @since 1.0.0
 */
public class StoreInfo {

    private String storeName;
    private String filter;

    public String getStoreName() {
        return storeName;
    }

    public String getFilter() {
        return filter;
    }

    public void setStoreName(String storeName) {
        this.storeName = storeName;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

}
