package com.alibaba.otter.canal.kafka.client.running;

import org.junit.Assert;

public class AbstractKafkaTest {

    protected String topic = "example";
    protected Integer partition = null;
    protected String groupId    = "g1";
    protected String servers    = "slave1.test.apitops.com:6667,slave2.test.apitops.com:6667,slave3.test.apitops.com:6667";

    public void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            Assert.fail(e.getMessage());
        }
    }
}
