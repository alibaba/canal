package com.alibaba.otter.canal.store;

import java.util.Map;

import org.springframework.util.Assert;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.google.common.collect.MapMaker;

/**
 * @author zebin.xuzb 2012-10-30 下午3:45:17
 * @since 1.0.0
 */
public abstract class AbstractCanalGroupStore<T> extends AbstractCanalLifeCycle implements CanalGroupEventStore<T> {

    protected Map<String, StoreInfo> stores = new MapMaker().makeMap();

    @Override
    public void addStoreInfo(StoreInfo info) {
        checkInfo(info);
        stores.put(info.getStoreName(), info);
    }

    protected void checkInfo(StoreInfo info) {
        Assert.notNull(info);
        Assert.hasText(info.getStoreName());
    }

}
