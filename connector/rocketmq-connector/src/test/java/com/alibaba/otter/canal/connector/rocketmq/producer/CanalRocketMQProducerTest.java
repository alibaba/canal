package com.alibaba.otter.canal.connector.rocketmq.producer;

import java.lang.reflect.Field;
import java.util.Properties;

import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class CanalRocketMQProducerTest {

    private CanalRocketMQProducer producer;

    @After
    public void tearDown() {
        if (producer != null) {
            producer.stop();
        }
    }

    @Test
    public void testAutoCreateTopicPlaceholderIsNotRefreshed() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("rocketmq.producer.group", "canal_ut_" + System.nanoTime());
        properties.setProperty("rocketmq.namesrv.addr", "127.0.0.1:10911");
        properties.setProperty("canal.mq.accessChannel", "cloud");
        properties.setProperty("canal.mq.topic", "canal_ut_topic");

        producer = new CanalRocketMQProducer();
        producer.init(properties);

        DefaultMQProducer defaultMQProducer = getDefaultMQProducer(producer);
        DefaultMQProducerImpl producerImpl = defaultMQProducer.getDefaultMQProducerImpl();

        // the auto-create template is still used when sending to a topic without route
        Assert.assertEquals(TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC, defaultMQProducer.getCreateTopicKey());
        // but its placeholder must not stay in the publish topic list, otherwise the scheduled
        // refresh keeps asking the name server for TBW102 and logs a warning every 30 seconds
        Assert.assertFalse(producerImpl.getTopicPublishInfoTable().containsKey(TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC));
        Assert.assertFalse(producerImpl.getMqClientFactory()
            .getDefaultMQProducer()
            .getDefaultMQProducerImpl()
            .getTopicPublishInfoTable()
            .containsKey(TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC));
    }

    private DefaultMQProducer getDefaultMQProducer(CanalRocketMQProducer canalProducer) throws Exception {
        Field field = CanalRocketMQProducer.class.getDeclaredField("defaultMQProducer");
        field.setAccessible(true);
        return (DefaultMQProducer) field.get(canalProducer);
    }
}
