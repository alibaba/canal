package com.alibaba.otter.canal.client.kafka;

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
        return new KafkaCanalConnector(servers, topic, partition, groupId, false);
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
        return new KafkaCanalConnector(servers, topic, null, groupId, false);
    }

    /**
     * 创建kafka客户端链接
     *
     * @param servers
     * @param topic
     * @param partition
     * @param groupId
     * @param flatMessage
     * @return
     */
    public static KafkaCanalConnector newKafkaConnector(String servers, String topic, Integer partition,
                                                        String groupId, boolean flatMessage) {
        return new KafkaCanalConnector(servers, topic, partition, groupId, flatMessage);
    }
}
