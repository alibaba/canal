package com.alibaba.otter.canal.connector.core.spi;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.alibaba.otter.canal.connector.core.consumer.CommonMessage;

/**
 * Canal/MQ consumer SPI 接口
 *
 * @author rewerma @ 2020-02-01
 * @version 1.0.0
 */
@SPI("kafka")
public interface CanalMsgConsumer {

    /**
     * 初始化
     * 
     * @param properties consumer properties
     * @param topic topic/destination
     * @param groupId mq group id
     */
    void init(Properties properties, String topic, String groupId);

    /**
     * 连接Canal/MQ
     */
    void connect();

    /**
     * 批量拉取数据
     * 
     * @param timeout 超时时间
     * @param unit 时间单位
     * @return Message列表
     */
    List<CommonMessage> getMessage(Long timeout, TimeUnit unit);

    /**
     * 提交
     */
    void ack();

    /**
     * 回滚
     */
    void rollback();

    /**
     * 断开连接
     */
    void disconnect();
}
