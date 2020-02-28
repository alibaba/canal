package com.alibaba.otter.canal.connector.core.spi;

import java.util.Properties;

import com.alibaba.otter.canal.connector.core.config.MQProperties;
import com.alibaba.otter.canal.connector.core.producer.MQDestination;
import com.alibaba.otter.canal.connector.core.util.Callback;
import com.alibaba.otter.canal.protocol.Message;

/**
 * MQ producer SPI 接口
 *
 * @author rewerma 2020-01-27
 * @version 1.0.0
 */
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
    void send(MQDestination canalDestination, Message message, Callback callback);

    /**
     * Stop MQ producer service
     */
    void stop();
}
