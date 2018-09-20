package com.alibaba.otter.canal.spi;

import com.alibaba.otter.canal.common.MQProperties;
import com.alibaba.otter.canal.protocol.Message;
import java.io.IOException;

public interface CanalMQProducer {
    /**
     * Init producer.
     *
     * @param mqProperties MQ config
     */
    void init(MQProperties mqProperties);

    /**
     * Send canal message to related topic
     *
     * @param topic MQ topic
     * @param message canal message
     * @throws IOException
     */
    void send(MQProperties.Topic topic, Message message) throws IOException;

    /**
     * Stop MQ producer service
     */
    void stop();
}