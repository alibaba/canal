package com.alibaba.otter.canal.parse.inbound;

import com.alibaba.otter.canal.common.CanalLifeCycle;

/**
 * 多阶段处理器接口类型
 *
 * @author wanghe Date: 2021/9/8 Time: 16:16
 */
public interface MultiStageCoprocessor extends CanalLifeCycle {

    /**
     * 发送数据到处理器，从此处开始数据进入处理流程
     *
     * @param data 进入处理器的原始数据
     * @return 放入是否成功的标识
     */
    boolean publish(Object data);
}
