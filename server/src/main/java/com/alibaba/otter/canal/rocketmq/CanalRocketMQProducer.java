package com.alibaba.otter.canal.rocketmq;

import com.alibaba.otter.canal.common.CanalMessageSerializer;
import com.alibaba.otter.canal.common.MQProperties;
import com.alibaba.otter.canal.server.exception.CanalServerException;
import com.alibaba.otter.canal.spi.CanalMQProducer;
import java.util.List;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CanalRocketMQProducer implements CanalMQProducer {

    private static final Logger logger = LoggerFactory.getLogger(CanalRocketMQProducer.class);

    private DefaultMQProducer   defaultMQProducer;

    @Override
    public void init(MQProperties rocketMQProperties) {
        defaultMQProducer = new DefaultMQProducer();
        defaultMQProducer.setNamesrvAddr(rocketMQProperties.getServers());
        defaultMQProducer.setProducerGroup(rocketMQProperties.getProducerGroup());
        defaultMQProducer.setRetryTimesWhenSendFailed(rocketMQProperties.getRetries());
        logger.info("##Start RocketMQ producer##");
        try {
            defaultMQProducer.start();
        } catch (MQClientException ex) {
            throw new CanalServerException("Start RocketMQ producer error", ex);
        }
    }

    @Override
    public void send(final MQProperties.CanalDestination destination, com.alibaba.otter.canal.protocol.Message data,
                     Callback callback) {
        try {
            Message message = new Message(destination.getTopic(), CanalMessageSerializer.serializer(data));
            this.defaultMQProducer.send(message, new MessageQueueSelector() {

                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    int partition = 0;
                    if (destination.getPartition() != null) {
                        partition = destination.getPartition();
                    }
                    return mqs.get(partition);
                }
            }, null);
            callback.commit();
        } catch (MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
            logger.error("Send message error!", e);
            callback.rollback();
        }
    }

    @Override
    public void stop() {
        logger.info("## Stop RocketMQ producer##");
        this.defaultMQProducer.shutdown();
    }
}
