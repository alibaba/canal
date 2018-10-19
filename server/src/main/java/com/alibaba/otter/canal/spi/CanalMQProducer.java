package com.alibaba.otter.canal.spi;

import java.io.IOException;

import com.alibaba.otter.canal.common.MQProperties;
import com.alibaba.otter.canal.protocol.Message;

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
     * @param canalDestination canal mq destination
     * @param message canal message
     * @throws IOException
     */
    void send(MQProperties.CanalDestination canalDestination, Message message, Callback callback) throws IOException;

    /**
     * Stop MQ producer service
     */
    void stop();

    interface Callback {

        void commit();

        void rollback();
    }
}
