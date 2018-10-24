package com.alibaba.otter.canal.client.adapter;

import java.util.List;

import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;
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
    void init(OuterAdapterConfig configuration);

    /**
     * 往适配器中同步数据
     *
     * @param dml 数据包
     */
    void sync(Dml dml);

    /**
     * 外部适配器销毁接口
     */
    void destroy();

    /**
     * Etl操作
     * 
     * @param task 任务名, 对应配置名
     * @param params etl筛选条件
     */
    default void etl(String task, List<String> params) {
        throw new UnsupportedOperationException();
    }

}
