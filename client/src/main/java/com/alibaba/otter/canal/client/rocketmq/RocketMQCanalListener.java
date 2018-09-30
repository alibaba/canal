package com.alibaba.otter.canal.client.rocketmq;

import java.util.List;

import org.apache.rocketmq.common.message.MessageExt;

/**
 * RocketMQ message listener
 */
public interface RocketMQCanalListener {

    boolean onReceive(List<MessageExt> messageExts);
}
