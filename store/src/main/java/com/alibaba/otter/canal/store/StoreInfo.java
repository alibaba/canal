/*
 * Copyright 2012 Alibaba.com All right reserved. This software is the
 * confidential and proprietary information of Alibaba.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Alibaba.com.
 */
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
