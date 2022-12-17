package com.alibaba.otter.canal.connector.pulsarmq.consumer;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.fastjson2.JSON;
import com.alibaba.otter.canal.connector.core.config.CanalConstants;
import com.alibaba.otter.canal.connector.core.consumer.CommonMessage;
import com.alibaba.otter.canal.connector.pulsarmq.config.PulsarMQConstants;

/**
 * @author chad
 * @date 2021/9/17 22:32
 * @since
 */
public class CanalPulsarMQConsumerTest {

    private Properties            properties;

    private CanalPulsarMQConsumer consumer;

    @Before
    public void before() {
        properties = new Properties();
        properties.setProperty(CanalConstants.CANAL_MQ_FLAT_MESSAGE, String.valueOf(true));
        properties.setProperty(CanalConstants.CANAL_MQ_CANAL_BATCH_SIZE, String.valueOf(30));
        properties.setProperty(PulsarMQConstants.PULSARMQ_GET_BATCH_TIMEOUT_SECONDS, String.valueOf(5));
        properties.setProperty(PulsarMQConstants.PULSARMQ_BATCH_PROCESS_TIMEOUT, String.valueOf(30));
        properties.setProperty(PulsarMQConstants.PULSARMQ_SERVER_URL, "pulsar://192.168.1.200:6650");
        properties.setProperty(PulsarMQConstants.PULSARMQ_ROLE_TOKEN, "123456");
        properties.setProperty(PulsarMQConstants.PULSARMQ_SUBSCRIPT_NAME, "test-for-canal-pulsar-consumer");
        properties.setProperty(PulsarMQConstants.PULSARMQ_REDELIVERY_DELAY_SECONDS, String.valueOf(30));
        properties.setProperty(PulsarMQConstants.PULSARMQ_ACK_TIMEOUT_SECONDS, String.valueOf(30));
        properties.setProperty(PulsarMQConstants.PULSARMQ_IS_RETRY, String.valueOf(true));
        properties.setProperty(PulsarMQConstants.PULSARMQ_IS_RETRY_DLQ_UPPERCASE, String.valueOf(true));
        properties.setProperty(PulsarMQConstants.PULSARMQ_MAX_REDELIVERY_COUNT, String.valueOf(16));

        consumer = new CanalPulsarMQConsumer();
        consumer.init(this.properties, "persistent://public/canal/test-topics-partition", "groupid");
    }

    @After
    public void after() {
        consumer.disconnect();
    }

    @Test
    public void getMessage() {
        consumer.connect();

        while (true) {
            List<CommonMessage> list = consumer.getMessage(-1L, TimeUnit.SECONDS);
            String time = LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
            if (null == list || list.isEmpty()) {
                System.out.println(time + " Receive empty");
                continue;
            }
            for (CommonMessage m : list) {
                System.out.println(time + " Receive ==> " + JSON.toJSONString(m));
            }

            consumer.ack();
        }
    }
}
