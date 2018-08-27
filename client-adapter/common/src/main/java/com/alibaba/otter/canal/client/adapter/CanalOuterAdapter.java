package com.alibaba.otter.canal.client.adapter;

import com.alibaba.otter.canal.client.adapter.support.CanalOuterAdapterConfiguration;
import com.alibaba.otter.canal.client.adapter.support.SPI;
import com.alibaba.otter.canal.protocol.Message;

/**
 * 外部适配器接口
 *
 * @author machengyuan 2018-8-18 下午10:14:02
 * @version 1.0.0
 */
@SPI("logger")
public interface CanalOuterAdapter {

    /**
     * 外部适配器初始化接口
     *
     * @param configuration 外部适配器配置信息
     */
    void init(CanalOuterAdapterConfiguration configuration);

    /**
     * 往适配器中写入数据
     *
     * @param message message数据包
     */
    void writeOut(Message message);

    /**
     * 外部适配器销毁接口
     */
    void destroy();
}
