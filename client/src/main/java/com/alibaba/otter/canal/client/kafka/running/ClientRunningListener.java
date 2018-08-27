package com.alibaba.otter.canal.client.kafka.running;

/**
 * client running状态信息
 *
 * @author machengyuan 2018-06-20 下午04:10:12
 * @version 1.0.0
 */
public interface ClientRunningListener {

    /**
     * 触发现在轮到自己做为active
     */
    public void processActiveEnter();

    /**
     * 触发一下当前active模式失败
     */
    public void processActiveExit();

}
