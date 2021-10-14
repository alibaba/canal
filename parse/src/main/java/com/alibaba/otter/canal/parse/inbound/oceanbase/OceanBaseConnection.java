package com.alibaba.otter.canal.parse.inbound.oceanbase;

import com.alibaba.otter.canal.parse.inbound.BinlogConnection;
import com.alibaba.otter.canal.parse.inbound.MultiStageCoprocessor;
import com.alibaba.otter.canal.parse.inbound.SinkFunction;

/**
 * OceanBase的BinlogConnection接口
 *
 * @author wanghe Date: 2021/9/8 Time: 16:16
 */
public interface OceanBaseConnection extends BinlogConnection {

    /**
     * 使用SinkFunction实例dump数据
     *
     * @param func 单线程的处理函数
     */
    void dump(SinkFunction func);

    /**
     * 使用MultiStageCoprocessor实例dump数据
     *
     * @param multiStageCoprocessor 多阶段处理器
     */
    void dump(MultiStageCoprocessor multiStageCoprocessor);
}
