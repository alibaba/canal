package com.alibaba.otter.canal.example.kafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.common.MQMessageUtils;
import com.alibaba.otter.canal.common.MQProperties;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.spi.CanalMQProducer.Callback;

public class IotBullKafkaProducer {
	private static final Logger logger = LoggerFactory.getLogger(IotBullKafkaProducer.class);

	private Producer<String, String> producer; // 用于扁平message的数据投递
	private MQProperties kafkaProperties;

	public void init(MQProperties kafkaProperties) {
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

		if (kafkaProperties.getTransaction()) {
			properties.put("transactional.id", "canal-transactional-id");
		} else {
			properties.put("retries", kafkaProperties.getRetries());
		}
		if (kafkaProperties.getFlatMessage()) {
			properties.put("value.serializer", StringSerializer.class.getName());
			producer = new KafkaProducer<String, String>(properties);
			if (kafkaProperties.getTransaction()) {
				producer.initTransactions();
			}
		}

	}

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
	}

	public void send(MQProperties.CanalDestination canalDestination, FlatMessage message, Callback callback) {
		// 开启事务，需要kafka版本支持
		Producer<String, String> producerTmp = producer;

		if (kafkaProperties.getTransaction()) {
			producerTmp.beginTransaction();
		}
		try {
			send(canalDestination, canalDestination.getTopic(), message);
			if (kafkaProperties.getTransaction()) {
				producerTmp.commitTransaction();
			}
			callback.commit();
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			if (kafkaProperties.getTransaction()) {
				producerTmp.abortTransaction();
			}
			callback.rollback();
		}
	}

	private void send(MQProperties.CanalDestination canalDestination, String topicName, FlatMessage flatMessage)
			throws Exception {
		if (kafkaProperties.getFlatMessage()) {
			// 发送扁平数据json

			if (flatMessage != null) {

				if (canalDestination.getPartitionHash() != null && !canalDestination.getPartitionHash().isEmpty()) {
					FlatMessage[] partitionFlatMessage = MQMessageUtils.messagePartition(flatMessage,
							canalDestination.getPartitionsNum(), canalDestination.getPartitionHash());
					int length = partitionFlatMessage.length;
					for (int i = 0; i < length; i++) {
						FlatMessage flatMessagePart = partitionFlatMessage[i];
						if (flatMessagePart != null) {
							produce(topicName, i, flatMessagePart);
						}
					}
				} else {
					final int partition = canalDestination.getPartition() != null ? canalDestination.getPartition() : 0;
					produce(topicName, partition, flatMessage);
				}

				if (logger.isDebugEnabled()) {
					logger.debug("Send flat message to kafka topic: [{}], packet: {}", topicName,
							JSON.toJSONString(flatMessage, SerializerFeature.WriteMapNullValue));
				}

			}
		}
	}

	private void produce(String topicName, int partition, FlatMessage flatMessage)
			throws ExecutionException, InterruptedException {
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, partition, null,
				JSON.toJSONString(flatMessage, SerializerFeature.WriteMapNullValue));
		if (kafkaProperties.getTransaction()) {
			producer.send(record);
		} else {
			producer.send(record);
		}
	}

}
