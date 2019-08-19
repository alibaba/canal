package com.alibaba.otter.canal.rabbitmq;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.mq.amqp.utils.UserUtils;
import com.alibaba.otter.canal.common.MQMessageUtils;
import com.alibaba.otter.canal.common.MQProperties;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.server.exception.CanalServerException;
import com.alibaba.otter.canal.spi.CanalMQProducer;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CanalRabbitMQProducer implements CanalMQProducer {

    private static final Logger logger = LoggerFactory.getLogger(CanalRabbitMQProducer.class);

    private MQProperties mqProperties;

    private Connection connection;

    private Channel channel;

    @Override
    public void init(MQProperties mqProperties) {
        try {
            this.mqProperties = mqProperties;
            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.setHost(mqProperties.getRabbitmqHost());
            connectionFactory.setPort(mqProperties.getRabbitmqPort());
            long ownerid = mqProperties.getRabbitmqOwnerid();
            if (ownerid == 0) {
                connectionFactory.setUsername(mqProperties.getRabbitmqUsername());
                connectionFactory.setPassword(mqProperties.getRabbitmqPassword());
            } else {
                connectionFactory.setUsername(UserUtils.getUserName(mqProperties.getRabbitmqUsername(), ownerid));
                connectionFactory.setPassword(UserUtils.getPassord(mqProperties.getRabbitmqPassword()));
            }
            connectionFactory.setVirtualHost(mqProperties.getRabbitmqVirtualhost());
            connectionFactory.setAutomaticRecoveryEnabled(true);
            connection = connectionFactory.newConnection();
            channel = connection.createChannel();
        } catch (Exception ex) {
            throw new CanalServerException("Start RabbitMQ connection error", ex);
        }
    }

    @Override
    public void send(MQProperties.CanalDestination destination, Message message, Callback callback) throws IOException {
        List<FlatMessage> flatMessages = MQMessageUtils.messageConverter(message);
        try {
            if (flatMessages != null) {
                if (!channel.isOpen()) {
                    channel = connection.createChannel();
                }
                for (FlatMessage flatMessage : flatMessages) {
                    logger.info("send flat message: {} to topic: {} fixed partition: {}",
                            JSON.toJSONString(flatMessage, SerializerFeature.WriteMapNullValue),
                            destination.getTopic(),
                            destination.getPartition());
                    String schema = flatMessage.getDatabase();
                    String table = flatMessage.getTable();
                    String type = flatMessage.getType();
                    if (!databases.contains(schema)) {
                        channel.exchangeDeclare(PREFIX + schema, "topic", true);
                        databases.add(schema);
                    }
                    channel.basicPublish(PREFIX + schema, type + "." + table, null,
                            JSON.toJSONString(flatMessage, SerializerFeature.WriteMapNullValue).getBytes("UTF-8"));
                }
            }
        } catch (Exception e) {
            logger.error("", e);
            callback.rollback();
            return;
        }

        callback.commit();
    }

    @Override
    public void stop() {
        try {
            logger.info("## stop the rabbitmq connection##");
            if (channel != null) {
                channel.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (Throwable e) {
            logger.warn("##something goes wrong when stopping rabbitmq connection:", e);
        } finally {
            logger.info("## rabbitmq connection is down.");
        }
    }

    private Set<String> databases = new HashSet<>();

    private static final String PREFIX = "com.alibaba.otter.canal-";
}
