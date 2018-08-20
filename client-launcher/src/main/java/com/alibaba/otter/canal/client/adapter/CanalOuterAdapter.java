package com.alibaba.otter.canal.client.adapter;

import com.alibaba.otter.canal.client.support.Dml;
import com.alibaba.otter.canal.client.support.SPI;

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
     * @param dml DML操作数据包
     * @return 是否成功
     */
    Boolean writeOut(Dml dml);

    /**
     * 外部适配器销毁接口
     */
    void destroy();
}
