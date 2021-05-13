package com.alibaba.otter.canal.example.kafka;

import com.alibaba.otter.canal.example.BaseCanalClientTest;

/**
 * Kafka 测试基类
 *
 * @author machengyuan @ 2018-6-12
 * @version 1.0.0
 */
public abstract class AbstractKafkaTest extends BaseCanalClientTest {

    public static String  topic     = "example";
    public static Integer partition = null;
    public static String  groupId   = "g4";
    public static String  servers   = "127.0.0.1:9092";
    public static String  zkServers = "127.0.0.1:2181";

    public void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
        }
    }
}
