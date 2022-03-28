package com.alibaba.otter.canal.connector.rocketmq.config;

/**
 * RocketMQ 配置常量类
 *
 * @author rewerma 2020-01-27
 * @version 1.0.0
 */
public class RocketMQConstants {

    public static final String ROOT                                  = "rocketmq";

    public static final String ROCKETMQ_PRODUCER_GROUP               = ROOT + "." + "producer.group";
    public static final String ROCKETMQ_ENABLE_MESSAGE_TRACE         = ROOT + "." + "enable.message.trace";
    public static final String ROCKETMQ_CUSTOMIZED_TRACE_TOPIC       = ROOT + "." + "customized.trace.topic";
    public static final String ROCKETMQ_NAMESPACE                    = ROOT + "." + "namespace";
    public static final String ROCKETMQ_NAMESRV_ADDR                 = ROOT + "." + "namesrv.addr";
    public static final String ROCKETMQ_RETRY_TIMES_WHEN_SEND_FAILED = ROOT + "." + "retry.times.when.send.failed";
    public static final String ROCKETMQ_VIP_CHANNEL_ENABLED          = ROOT + "." + "vip.channel.enabled";
    public static final String ROCKETMQ_TAG                          = ROOT + "." + "tag";

    public static final String ROCKETMQ_ACCESS_CHANNEL               = ROOT + "." + "access.channel";
    public static final String ROCKETMQ_BATCH_SIZE                   = ROOT + "." + "batch.size";
    public static final String ROCKETMQ_SUBSCRIBE_FILTER             = ROOT + "." + "subscribe.filter";

}
