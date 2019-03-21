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
    public static String  servers   = "slave1:6667,slave2:6667,slave3:6667";
    public static String  zkServers = "slave1:2181,slave2:2181,slave3:2181";

    public void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
        }
    }
}
