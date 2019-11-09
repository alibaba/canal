package com.alibaba.otter.canal.rabbitmq;

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
import com.alibaba.otter.canal.spi.CanalMQProducer;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CanalRabbitMQProducer extends AbstractMQProducer implements CanalMQProducer {

    private static final Logger logger = LoggerFactory.getLogger(CanalRabbitMQProducer.class);
    private MQProperties        mqProperties;
    private Connection          connect;
    private Channel             channel;
    private List<ConnectionFactory> connectionFactoryList;
    @Override
    public void init(MQProperties mqProperties) {
        super.init(mqProperties);
        this.mqProperties = mqProperties;
        connectionFactoryList = buildConnectionFactoryList(mqProperties);
        //初始化rabbitmq channel
        initChannel();
        if (Objects.isNull(channel)) {
            throw new IllegalStateException("rabbitmq channel init fail. mq.servers="+mqProperties.getServers());
        }
        try {
            //声明exchange
            channel.exchangeDeclare(mqProperties.getExchange(), BuiltinExchangeType.TOPIC.getType(),true);
        } catch (Exception e) {
            throw new IllegalStateException("rabbitmq declare exchange fail",e);
        }
    }

    private synchronized void initChannel() {
        for (ConnectionFactory connectionFactory : connectionFactoryList) {
            if (Objects.nonNull(connect)) {
                try {
                    connect.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            try {
                connect = connectionFactory.newConnection();
                channel = connect.createChannel();
                break;
            } catch (Exception ignored) {

            }
        }
    }


    private List<ConnectionFactory> buildConnectionFactoryList(MQProperties mqProperties) {
        List<ConnectionFactory> connectionFactoryList = Lists.newLinkedList();
        if (StringUtils.isBlank(mqProperties.getServers())) {
            throw new IllegalArgumentException("invalid canal.mq.servers, format is host:port,...,hostN:portN");
        }
        //解析rabbitmq集群列表
        final String[] hostPortArr = mqProperties.getServers().split(",");
        for (String string : hostPortArr) {
            if (StringUtils.isBlank(string)) {
                throw new IllegalArgumentException("invalid canal.mq.servers, format is host:port,...,hostN:portN");
            }
            final HostAndPort hostAndPort = HostAndPort.fromString(string);
            if (!hostAndPort.hasPort()) {
                throw new IllegalArgumentException("invalid canal.mq.servers, format is host:port,...,hostN:portN");
            }

            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(hostAndPort.getHostText());
            factory.setPort(hostAndPort.getPort());
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
            connectionFactoryList.add(factory);
        }

        return connectionFactoryList;
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

                    template.submit(() -> send(canalDestination, topicName, messageSub));
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

    private void closeChannelAndConnection(){
        if (Objects.nonNull(channel)) {
            try {
                channel.close();
            } catch (Exception ignored) {
            }
        }
        if (Objects.nonNull(connect)) {
            try {
                connect.close();
            } catch (Exception ignored) {
            }
        }
    }

    private void sendMessage(String queueName, byte[] message) {
        if (Objects.isNull(channel)) {
            initChannel();
        }
        if (Objects.isNull(channel)) {
            throw new IllegalStateException("rabbitmq channel init fail. mq.servers="+mqProperties.getServers());
        }
        // tips: 目前逻辑中暂不处理对exchange处理，请在Console后台绑定 才可使用routekey
        try {
            channel.basicPublish(mqProperties.getExchange(), queueName, null, message);
        } catch (IOException e) {
            closeChannelAndConnection();
        }
    }

    @Override
    public void stop() {
        logger.info("## Stop RabbitMQ producer##");
        closeChannelAndConnection();
        super.stop();
    }
}
