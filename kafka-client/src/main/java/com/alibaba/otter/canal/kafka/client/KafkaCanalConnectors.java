package com.alibaba.otter.canal.kafka.client;

/**
 * canal kafka connectors创建工具类
 *
 * @author machengyuan @ 2018-6-12
 * @version 1.0.0
 */
public class KafkaCanalConnectors {

    /**
     * 创建kafka客户端链接，独立运行不注册zk信息
     *
     * @param servers
     * @param topic
     * @param partition
     * @param groupId
     * @return
     */
    public static KafkaCanalConnector newKafkaConnector(String servers, String topic, Integer partition, String groupId) {
        return new KafkaCanalConnector(null, servers, topic, partition, groupId);
    }

    /**
     * 创建kafka客户端链接，独立运行不注册zk信息
     *
     * @param servers
     * @param topic
     * @param groupId
     * @return
     */
    public static KafkaCanalConnector newKafkaConnector(String servers, String topic, String groupId) {
        return new KafkaCanalConnector(null, servers, topic, null, groupId);
    }

    /**
     * 创建kafka客户端链接
     *
     * @param zkServers
     * @param servers
     * @param topic
     * @param partition
     * @param groupId
     * @return
     */
    public static KafkaCanalConnector newKafkaConnector(String zkServers, String servers, String topic, Integer partition, String groupId) {
        return new KafkaCanalConnector(zkServers, servers, topic, partition, groupId);
    }

    /**
     * 创建kafka客户端链接
     *
     * @param zkServers
     * @param servers
     * @param topic
     * @param groupId
     * @return
     */
    public static KafkaCanalConnector newKafkaConnector(String zkServers, String servers, String topic, String groupId) {
        return new KafkaCanalConnector(zkServers, servers, topic, null, groupId);
    }
}
