package com.alibaba.otter.canal.connector.pulsarmq.config;

/**
 * PulsarMQ配置
 *
 * @author chad
 * @date 2021/9/15 11:13
 * @since 1 by chad at 2021/9/15 新增配置文件
 */
public class PulsarMQConstants {

    public static final String ROOT                               = "pulsarmq";
    /**
     * pulsar服务连接地址
     */
    public static final String PULSARMQ_SERVER_URL                = ROOT + "." + "serverUrl";
    /**
     * pulsar服务角色token，需要有对应token的生产者权限
     */
    public static final String PULSARMQ_ROLE_TOKEN                = ROOT + "." + "roleToken";
    /**
     * topic前缀
     */
    public static final String PULSARMQ_TOPIC_TENANT_PREFIX       = ROOT + "." + "topicTenantPrefix";

    /**** 消费者 *****/
    /**
     * 获取批量消息超时等待时间
     */
    public static final String PULSARMQ_GET_BATCH_TIMEOUT_SECONDS = ROOT + "." + "getBatchTimeoutSeconds";
    /**
     * 批量处理超时时间
     */
    public static final String PULSARMQ_BATCH_PROCESS_TIMEOUT     = ROOT + "." + "batchProcessTimeout";
    /**
     * 消费都订阅名称，将以该名称为消费者身份标识，同一个subscriptName，认为是同一个消费实例
     */
    public static final String PULSARMQ_SUBSCRIPT_NAME            = ROOT + "." + "subscriptName";
    /**
     * 重试间隔秒数
     */
    public static final String PULSARMQ_REDELIVERY_DELAY_SECONDS  = ROOT + "." + "redeliveryDelaySeconds";
    /**
     * ACK超时秒数
     */
    public static final String PULSARMQ_ACK_TIMEOUT_SECONDS       = ROOT + "." + "ackTimeoutSeconds";
    /**
     * 是否开启消费重试
     */
    public static final String PULSARMQ_IS_RETRY                  = ROOT + "." + "isRetry";
    /**
     * 自动生成的 retry dlq队列名称后缀是否大写
     */
    public static final String PULSARMQ_IS_RETRY_DLQ_UPPERCASE    = ROOT + "." + "isRetryDLQUpperCase";
    /**
     * 最大重试次数
     */
    public static final String PULSARMQ_MAX_REDELIVERY_COUNT      = ROOT + "." + "maxRedeliveryCount";
    /**
     * Pulsar admin服务器地址
     */
    public static final String PULSARMQ_ADMIN_SERVER_URL          = ROOT + "." + "adminServerUrl";

}
