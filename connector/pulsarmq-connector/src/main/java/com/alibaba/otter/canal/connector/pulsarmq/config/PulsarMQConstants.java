package com.alibaba.otter.canal.connector.pulsarmq.config;

/**
 * PulsarMQ配置
 * @author chad
 * @date 2021/9/15 11:13
 * @since 1 by chad at 2021/9/15 新增配置文件
 */
public class PulsarMQConstants {
    public static final String ROOT                                  = "pulsarmq";
    /**
     * pulsar服务连接地址
     */
    public static final String PULSARMQ_SERVER_URL                 = ROOT + "." + "serverUrl";
    /**
     * pulsar服务角色token，需要有对应token的生产者权限
     */
    public static final String PULSARMQ_ROLE_TOKEN             = ROOT + "." + "roleToken";
    /**
     * topic前缀
     */
    public static final String PULSARMQ_TOPIC_TENANT_PREFIX = ROOT + "." + "topicTenantPrefix";
}
