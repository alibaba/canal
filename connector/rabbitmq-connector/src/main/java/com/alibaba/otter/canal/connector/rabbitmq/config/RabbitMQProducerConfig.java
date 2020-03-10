package com.alibaba.otter.canal.connector.rabbitmq.config;

import com.alibaba.otter.canal.connector.core.config.MQProperties;

/**
 * RabbitMQ 配置类
 *
 * @author rewerma 2020-01-27
 * @version 1.0.0
 */
public class RabbitMQProducerConfig extends MQProperties {

    private String host;
    private String virtualHost;
    private String exchange;
    private String username;
    private String password;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getVirtualHost() {
        return virtualHost;
    }

    public void setVirtualHost(String virtualHost) {
        this.virtualHost = virtualHost;
    }

    public String getExchange() {
        return exchange;
    }

    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
