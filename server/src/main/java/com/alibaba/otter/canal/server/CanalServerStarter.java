package com.alibaba.otter.canal.server;

/**
 * 外部服务如Kafka, RocketMQ启动接口
 *
 * @author machengyuan 2018-8-23 下午05:20:29
 * @version 1.0.0
 */
public interface CanalServerStarter {

    void init();
}
