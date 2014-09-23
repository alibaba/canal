package com.alibaba.otter.canal.store;

/**
 * 提供给上层统一的 store 视图，内部则支持多种store混合，并且维持着多个store供上层进行路由
 * 
 * @author zebin.xuzb 2012-10-30 下午12:17:26
 * @since 1.0.0
 */
public interface CanalGroupEventStore<T> extends CanalEventStore<T> {

    void addStoreInfo(StoreInfo info);
}
