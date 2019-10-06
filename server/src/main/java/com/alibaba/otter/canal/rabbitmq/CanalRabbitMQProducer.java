package com.alibaba.otter.canal.rabbitmq;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.common.AbstractMQProducer;
import com.alibaba.otter.canal.common.CanalMessageSerializer;
import com.alibaba.otter.canal.common.MQMessageUtils;
import com.alibaba.otter.canal.common.MQMessageUtils.EntryRowData;
import com.alibaba.otter.canal.common.MQProperties;
import com.alibaba.otter.canal.common.utils.ExecutorTemplate;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.server.exception.CanalServerException;
import com.alibaba.otter.canal.spi.CanalMQProducer;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class CanalRabbitMQProducer extends AbstractMQProducer implements CanalMQProducer {

    private static final Logger logger = LoggerFactory.getLogger(CanalRabbitMQProducer.class);
    private MQProperties        mqProperties;
    private Connection          connect;
    private Channel             channel;

    @Override
    public void init(MQProperties mqProperties) {
        super.init(mqProperties);
        this.mqProperties = mqProperties;
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(mqProperties.getServers());
        if (mqProperties.getAliyunAccessKey().length() > 0 && mqProperties.getAliyunSecretKey().length() > 0
            && mqProperties.getAliyunUID() > 0) {
            factory.setCredentialsProvider(new AliyunCredentialsProvider(mqProperties.getAliyunAccessKey(),
                mqProperties.getAliyunSecretKey(),
                mqProperties.getAliyunUID()));
        } else {
            factory.setUsername(mqProperties.getUsername());
            factory.setPassword(mqProperties.getPassword());
        }
        factory.setVirtualHost(mqProperties.getVhost());
        try {
            connect = factory.newConnection();
            channel = connect.createChannel();
            // channel.exchangeDeclare(mqProperties.getExchange(), "topic");
        } catch (IOException | TimeoutException ex) {
            throw new CanalServerException("Start RabbitMQ producer error", ex);
        }
    }

    @Override
    public void send(final MQProperties.CanalDestination canalDestination, Message message, Callback callback)
                                                                                                              throws IOException {
        ExecutorTemplate template = new ExecutorTemplate(executor);
        try {
            if (!StringUtils.isEmpty(canalDestination.getDynamicTopic())) {
                // 动态topic
                Map<String, com.alibaba.otter.canal.protocol.Message> messageMap = MQMessageUtils.messageTopics(message,
                    canalDestination.getTopic(),
                    canalDestination.getDynamicTopic());

                for (Map.Entry<String, com.alibaba.otter.canal.protocol.Message> entry : messageMap.entrySet()) {
                    final String topicName = entry.getKey().replace('.', '_');
                    final com.alibaba.otter.canal.protocol.Message messageSub = entry.getValue();

                    template.submit(new Runnable() {

                        @Override
                        public void run() {
                            send(canalDestination, topicName, messageSub);
                        }
                    });
                }

                template.waitForResult();
            } else {
                send(canalDestination, canalDestination.getTopic(), message);
            }
            callback.commit();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            callback.rollback();
        } finally {
            template.clear();
        }
    }

    private void send(MQProperties.CanalDestination canalDestination, String topicName, Message messageSub) {
        if (!mqProperties.getFlatMessage()) {
            byte[] message = CanalMessageSerializer.serializer(messageSub, mqProperties.isFilterTransactionEntry());
            if (logger.isDebugEnabled()) {
                logger.debug("send message:{} to destination:{}", message, canalDestination.getCanalDestination());
            }
            sendMessage(topicName, message);
        } else {
            // 并发构造
            EntryRowData[] datas = MQMessageUtils.buildMessageData(messageSub, executor);
            // 串行分区
            List<FlatMessage> flatMessages = MQMessageUtils.messageConverter(datas, messageSub.getId());
            if (flatMessages != null) {
                for (FlatMessage flatMessage : flatMessages) {
                    byte[] message = JSON.toJSONBytes(flatMessage, SerializerFeature.WriteMapNullValue);
                    if (logger.isDebugEnabled()) {
                        logger.debug("send message:{} to destination:{}",
                            message,
                            canalDestination.getCanalDestination());
                    }
                    sendMessage(topicName, message);
                }
            }
        }

    }

    private void sendMessage(String queueName, byte[] message) {
        // tips: 目前逻辑中暂不处理对exchange处理，请在Console后台绑定 才可使用routekey
        try {
            channel.basicPublish(mqProperties.getExchange(), queueName, null, message);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        logger.info("## Stop RabbitMQ producer##");
        try {
            this.connect.close();
            this.channel.close();
        } catch (IOException | TimeoutException ex) {
            throw new CanalServerException("Stop RabbitMQ producer error", ex);
        }

        super.stop();
    }
}
