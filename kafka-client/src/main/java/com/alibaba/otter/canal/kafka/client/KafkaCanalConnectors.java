package com.alibaba.otter.canal.kafka.client;

public class KafkaCanalConnectors {
    /**
     * 创建kafka客户端链接
     *
     * @param servers
     * @param topic
     * @param partition
     * @param groupId
     * @return
     */
    public static KafkaCanalConnector newKafkaConnector(String servers, String topic, Integer partition, String groupId) {
        return new KafkaCanalConnector(servers, topic, partition, groupId);
    }

    /**
     * 创建kafka客户端链接
     *
     * @param servers
     * @param topic
     * @param groupId
     * @return
     */
    public static KafkaCanalConnector newKafkaConnector(String servers, String topic,  String groupId) {
        return new KafkaCanalConnector(servers, topic, null, groupId);
    }
}
