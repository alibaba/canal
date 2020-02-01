package com.alibaba.otter.canal.connector.rabbitmq.config;

/**
 * RabbitMQ 配置常量类
 *
 * @author rewerma 2020-01-27
 * @version 1.0.0
 */
public class RabbitMQConstants {

    public static final String ROOT                      = "rabbitmq";

    public static final String RABBITMQ_HOST             = ROOT + "." + "host";
    public static final String RABBITMQ_EXCHANGE         = ROOT + "." + "exchange";
    public static final String RABBITMQ_VIRTUAL_HOST     = ROOT + "." + "virtual.host";
    public static final String RABBITMQ_USERNAME         = ROOT + "." + "username";
    public static final String RABBITMQ_PASSWORD         = ROOT + "." + "password";

    public static final String RABBITMQ_RESOURCE_OWNERID = ROOT + "." + "rabbitmq.resource.ownerId";
}
