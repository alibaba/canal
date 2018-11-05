package com.alibaba.otter.canal.rocketmq;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.common.CanalMessageSerializer;
import com.alibaba.otter.canal.common.MQProperties;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.server.exception.CanalServerException;
import com.alibaba.otter.canal.spi.CanalMQProducer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CanalRocketMQProducer implements CanalMQProducer {

    private static final Logger logger = LoggerFactory.getLogger(CanalRocketMQProducer.class);

    private DefaultMQProducer   defaultMQProducer;

    private MQProperties        mqProperties;

    @Override
    public void init(MQProperties rocketMQProperties) {
        this.mqProperties = rocketMQProperties;
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
        if (!mqProperties.getFlatMessage()) {
            try {
                Message message = new Message(destination.getTopic(), CanalMessageSerializer.serializer(data,
                    mqProperties.isFilterTransactionEntry()));
                logger.debug("send message:{} to destination:{}, partition: {}",
                    message,
                    destination.getCanalDestination(),
                    destination.getPartition());
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
            } catch (MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
                logger.error("Send message error!", e);
                callback.rollback();
                return;
            }
        } else {
            List<FlatMessage> flatMessages = FlatMessage.messageConverter(data);
            if (flatMessages != null) {
                for (FlatMessage flatMessage : flatMessages) {
                    if (destination.getPartition() != null) {
                        try {
                            logger.info("send flat message: {} to topic: {} fixed partition: {}",
                                JSON.toJSONString(flatMessage),
                                destination.getTopic(),
                                destination.getPartition());
                            Message message = new Message(destination.getTopic(),
                                JSON.toJSONString(flatMessage).getBytes());
                            this.defaultMQProducer.send(message, new MessageQueueSelector() {

                                @Override
                                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                                    return mqs.get(destination.getPartition());
                                }
                            }, null);
                        } catch (Exception e) {
                            logger.error("send flat message to fixed partition error", e);
                            callback.rollback();
                            return;
                        }
                    } else {
                        if (destination.getPartitionHash() != null && !destination.getPartitionHash().isEmpty()) {
                            FlatMessage[] partitionFlatMessage = FlatMessage.messagePartition(flatMessage,
                                destination.getPartitionsNum(),
                                destination.getPartitionHash());
                            int length = partitionFlatMessage.length;
                            for (int i = 0; i < length; i++) {
                                FlatMessage flatMessagePart = partitionFlatMessage[i];
                                if (flatMessagePart != null) {
                                    logger.debug("flatMessagePart: {}, partition: {}",
                                        JSON.toJSONString(flatMessagePart),
                                        i);
                                    final int index = i;
                                    try {
                                        Message message = new Message(destination.getTopic(),
                                            JSON.toJSONString(flatMessagePart).getBytes());
                                        this.defaultMQProducer.send(message, new MessageQueueSelector() {

                                            @Override
                                            public MessageQueue select(List<MessageQueue> mqs, Message msg,
                                                                       Object arg) {
                                                if (index > mqs.size()) {
                                                    throw new CanalServerException(
                                                        "partition number is error,config num:"
                                                                                   + destination.getPartitionsNum()
                                                                                   + ", mq num: " + mqs.size());
                                                }
                                                return mqs.get(index);
                                            }
                                        }, null);
                                    } catch (Exception e) {
                                        logger.error("send flat message to hashed partition error", e);
                                        callback.rollback();
                                        return;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        callback.commit();
        if (logger.isDebugEnabled()) {
            logger.debug("send message to rocket topic: {}", destination.getTopic());
        }
    }

    @Override
    public void stop() {
        logger.info("## Stop RocketMQ producer##");
        this.defaultMQProducer.shutdown();
    }
}
