package com.alibaba.otter.canal.connector.core.spi;

import java.io.IOException;
import java.util.Properties;

import com.alibaba.otter.canal.connector.core.config.MQProperties;
import com.alibaba.otter.canal.connector.core.producer.Callback;
import com.alibaba.otter.canal.connector.core.producer.MQDestination;
import com.alibaba.otter.canal.protocol.Message;

@SPI("kafka")
public interface CanalMQProducer {

    /**
     * Init producer.
     */
    void init(Properties properties);

    /**
     * Get base mq properties
     * 
     * @return MQProperties
     */
    MQProperties getMqProperties();

    /**
     * Send canal message to related topic
     *
     * @param canalDestination canal mq destination
     * @param message canal message
     */
    void send(MQDestination canalDestination, Message message, Callback callback) throws IOException;

    /**
     * Stop MQ producer service
     */
    void stop();
}
