package com.alibaba.otter.canal.client.adapter;

import java.util.Map;

import com.alibaba.otter.canal.client.adapter.support.CanalOuterAdapterConfiguration;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.SPI;

/**
 * 外部适配器接口
 *
 * @author machengyuan 2018-8-18 下午10:14:02
 * @version 1.0.0
 */
@SPI("logger")
public interface OuterAdapter {

    /**
     * 外部适配器初始化接口
     *
     * @param configuration 外部适配器配置信息
     */
    void init(CanalOuterAdapterConfiguration configuration);

    /**
     * 往适配器中同步数据
     *
     * @param dml 数据包
     */
    void sync(Dml dml);

    // void writeOut(FlatMessage flatMessage);

    /**
     * 外部适配器销毁接口
     */
    void destroy();

    /**
     * Etl操作
     * 
     * @param criteriaSql 条件拼接sql
     */
    default void etl(String criteriaSql) {
        throw new UnsupportedOperationException();
    }

}
