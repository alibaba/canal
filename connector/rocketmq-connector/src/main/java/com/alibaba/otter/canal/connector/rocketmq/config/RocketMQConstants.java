package com.alibaba.otter.canal.connector.rocketmq.config;

/**
 * 启动常用变量
 *
 * @author jianghang 2012-11-8 下午03:15:55
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

}
