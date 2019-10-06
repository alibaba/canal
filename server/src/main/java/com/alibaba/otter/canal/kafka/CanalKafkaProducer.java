package com.alibaba.otter.canal.kafka;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
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
import com.alibaba.otter.canal.spi.CanalMQProducer;

/**
 * kafka producer 主操作类
 *
 * @author machengyuan 2018-6-11 下午05:30:49
 * @version 1.0.0
 */
public class CanalKafkaProducer extends AbstractMQProducer implements CanalMQProducer {

    private static final Logger      logger = LoggerFactory.getLogger(CanalKafkaProducer.class);

    private Producer<String, byte[]> producer;
    private MQProperties             kafkaProperties;

    @Override
    public void init(MQProperties kafkaProperties) {
        super.init(kafkaProperties);

        this.kafkaProperties = kafkaProperties;
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaProperties.getServers());
        properties.put("acks", kafkaProperties.getAcks());
        properties.put("compression.type", kafkaProperties.getCompressionType());
        properties.put("batch.size", kafkaProperties.getBatchSize());
        properties.put("linger.ms", kafkaProperties.getLingerMs());
        properties.put("max.request.size", kafkaProperties.getMaxRequestSize());
        properties.put("buffer.memory", kafkaProperties.getBufferMemory());
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("max.in.flight.requests.per.connection", 1);

        if (!kafkaProperties.getProperties().isEmpty()) {
            properties.putAll(kafkaProperties.getProperties());
        }
        properties.put("retries", kafkaProperties.getRetries());
        if (kafkaProperties.isKerberosEnable()) {
            File krb5File = new File(kafkaProperties.getKerberosKrb5FilePath());
            File jaasFile = new File(kafkaProperties.getKerberosJaasFilePath());
            if (krb5File.exists() && jaasFile.exists()) {
                // 配置kerberos认证，需要使用绝对路径
                System.setProperty("java.security.krb5.conf", krb5File.getAbsolutePath());
                System.setProperty("java.security.auth.login.config", jaasFile.getAbsolutePath());
                System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
                properties.put("security.protocol", "SASL_PLAINTEXT");
                properties.put("sasl.kerberos.service.name", "kafka");
            } else {
                String errorMsg = "ERROR # The kafka kerberos configuration file does not exist! please check it";
                logger.error(errorMsg);
                throw new RuntimeException(errorMsg);
            }
        }

        properties.put("value.serializer", KafkaMessageSerializer.class.getName());
        producer = new KafkaProducer<String, byte[]>(properties);
    }

    @Override
    public void stop() {
        try {
            logger.info("## stop the kafka producer");
            if (producer != null) {
                producer.close();
            }
        } catch (Throwable e) {
            logger.warn("##something goes wrong when stopping kafka producer:", e);
        } finally {
            logger.info("## kafka producer is down.");
        }

        super.stop();
    }

    @Override
    public void send(MQProperties.CanalDestination canalDestination, Message message, Callback callback) {
        ExecutorTemplate template = new ExecutorTemplate(executor);
        boolean flat = kafkaProperties.getFlatMessage();

        try {
            List result = null;
            if (!StringUtils.isEmpty(canalDestination.getDynamicTopic())) {
                // 动态topic路由计算,只是基于schema/table,不涉及proto数据反序列化
                Map<String, Message> messageMap = MQMessageUtils.messageTopics(message,
                    canalDestination.getTopic(),
                    canalDestination.getDynamicTopic());

                // 针对不同的topic,引入多线程提升效率
                for (Map.Entry<String, Message> entry : messageMap.entrySet()) {
                    final String topicName = entry.getKey().replace('.', '_');
                    final Message messageSub = entry.getValue();
                    template.submit(new Callable() {

                        @Override
                        public List<Future> call() throws Exception {
                            try {
                                return send(canalDestination, topicName, messageSub, flat);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });
                }

                result = template.waitForResult();
            } else {
                result = new ArrayList();
                List<Future> futures = send(canalDestination, canalDestination.getTopic(), message, flat);
                result.add(futures);
            }

            // 一个批次的所有topic和分区的队列，都采用异步的模式进行多线程批量发送
            // 最后在集结点进行flush等待，确保所有数据都写出成功
            // 注意：kafka的异步模式如果要保证顺序性，需要设置max.in.flight.requests.per.connection=1，确保在网络异常重试时有排他性
            producer.flush();
            // flush操作也有可能是发送失败,这里需要异步关注一下发送结果,针对有异常的直接出发rollback
            for (Object obj : result) {
                List<Future> futures = (List<Future>) obj;
                for (Future future : futures) {
                    try {
                        future.get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            callback.commit();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            callback.rollback();
        } finally {
            template.clear();
        }
    }

    private List<Future> send(MQProperties.CanalDestination canalDestination, String topicName, Message message,
                              boolean flat) throws Exception {
        List<ProducerRecord<String, byte[]>> records = new ArrayList<ProducerRecord<String, byte[]>>();
        if (!flat) {
            if (canalDestination.getPartitionHash() != null && !canalDestination.getPartitionHash().isEmpty()) {
                // 并发构造
                EntryRowData[] datas = MQMessageUtils.buildMessageData(message, executor);
                // 串行分区
                Message[] messages = MQMessageUtils.messagePartition(datas,
                    message.getId(),
                    canalDestination.getPartitionsNum(),
                    canalDestination.getPartitionHash());
                int length = messages.length;
                for (int i = 0; i < length; i++) {
                    Message messagePartition = messages[i];
                    if (messagePartition != null) {
                        records.add(new ProducerRecord<String, byte[]>(topicName,
                            i,
                            null,
                            CanalMessageSerializer.serializer(messagePartition,
                                kafkaProperties.isFilterTransactionEntry())));
                    }
                }
            } else {
                final int partition = canalDestination.getPartition() != null ? canalDestination.getPartition() : 0;
                records.add(new ProducerRecord<String, byte[]>(topicName,
                    partition,
                    null,
                    CanalMessageSerializer.serializer(message, kafkaProperties.isFilterTransactionEntry())));
            }
        } else {
            // 发送扁平数据json
            // 并发构造
            EntryRowData[] datas = MQMessageUtils.buildMessageData(message, executor);
            // 串行分区
            List<FlatMessage> flatMessages = MQMessageUtils.messageConverter(datas, message.getId());
            for (FlatMessage flatMessage : flatMessages) {
                if (canalDestination.getPartitionHash() != null && !canalDestination.getPartitionHash().isEmpty()) {
                    FlatMessage[] partitionFlatMessage = MQMessageUtils.messagePartition(flatMessage,
                        canalDestination.getPartitionsNum(),
                        canalDestination.getPartitionHash());
                    int length = partitionFlatMessage.length;
                    for (int i = 0; i < length; i++) {
                        FlatMessage flatMessagePart = partitionFlatMessage[i];
                        if (flatMessagePart != null) {
                            records.add(new ProducerRecord<String, byte[]>(topicName,
                                i,
                                null,
                                JSON.toJSONBytes(flatMessagePart, SerializerFeature.WriteMapNullValue)));
                        }
                    }
                } else {
                    final int partition = canalDestination.getPartition() != null ? canalDestination.getPartition() : 0;
                    records.add(new ProducerRecord<String, byte[]>(topicName,
                        partition,
                        null,
                        JSON.toJSONBytes(flatMessage, SerializerFeature.WriteMapNullValue)));
                }
            }
        }

        return produce(topicName, records, flat);
    }

    private List<Future> produce(String topicName, List<ProducerRecord<String, byte[]>> records, boolean flatMessage) {
        List<Future> futures = new ArrayList<Future>();
        // 异步发送，因为在partition hash的时候已经按照每个分区合并了消息，走到这一步不需要考虑单个分区内的顺序问题
        for (ProducerRecord record : records) {
            futures.add(producer.send(record));
        }

        return futures;
    }

}
