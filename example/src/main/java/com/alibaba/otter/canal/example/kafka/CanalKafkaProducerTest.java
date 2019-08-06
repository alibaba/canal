package com.alibaba.otter.canal.example.kafka;

import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.common.MQProperties;
import com.alibaba.otter.canal.common.MQProperties.CanalDestination;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.spi.CanalMQProducer;

public class CanalKafkaProducerTest {
	private static final Logger logger = LoggerFactory.getLogger(CanalKafkaProducerTest.class);
	private IotBullKafkaProducer canalMQProducer = null;

	private static MQProperties buildMQProperties(Properties properties) {
		MQProperties mqProperties = new MQProperties();

		mqProperties.setServers("paascloud-kafka:9092");

		mqProperties.setRetries(Integer.valueOf(0));

		mqProperties.setBatchSize(Integer.valueOf(10));

		mqProperties.setLingerMs(Integer.valueOf(10));

		mqProperties.setMaxRequestSize(Integer.valueOf(1048576));

		mqProperties.setBufferMemory(Long.valueOf(33554432));

		mqProperties.setCanalBatchSize(Integer.valueOf(50));

		mqProperties.setCanalGetTimeout(Long.valueOf(100));

		mqProperties.setFlatMessage(Boolean.valueOf(true));

		mqProperties.setCompressionType("none");

		mqProperties.setAcks("all");

		// mqProperties.setAliyunAccessKey(aliyunAccessKey);

		// mqProperties.setAliyunSecretKey(aliyunSecretKey);

		mqProperties.setTransaction(Boolean.valueOf("true"));

		mqProperties.setProducerGroup("canal-producer");

		return mqProperties;
	}

	public void testProduce() {
		if (canalMQProducer == null) {
			canalMQProducer = new IotBullKafkaProducer();

			MQProperties properties = buildMQProperties(null);
			System.setProperty("canal.destinations", "paascloud");
			// set filterTransactionEntry
			if (properties.isFilterTransactionEntry()) {
				System.setProperty("canal.instance.filter.transaction.entry", "true");
			}
			canalMQProducer.init(properties);
			
			
			String[] destinations = StringUtils.split(System.getProperty("canal.destinations"), ",");
			for (String destination : destinations) {
				destination = destination.trim();
				FlatMessage message = new FlatMessage(1);
				MQProperties.CanalDestination canalDestination = new MQProperties.CanalDestination();
				canalDestination.setCanalDestination(destination);
				message.setDatabase("paascloud_pdc");
				message.setType("select");
				message.setTable("pc_table");
				canalDestination.setTopic(message.getDatabase() + "_" + message.getTable());
				canalDestination.setPartition(0);

				this.sendMsg(canalDestination, message, canalMQProducer);
			}

		}

	}

	private void sendMsg(CanalDestination canalDestination, FlatMessage message, IotBullKafkaProducer producer) {

		final long batchId = message.getId();
		try {

			if (batchId != -1) {
				producer.send(canalDestination, message, new CanalMQProducer.Callback() {

					@Override
					public void commit() {
						logger.info("send msg successfully");
					}

					@Override
					public void rollback() {
						logger.info("send msg failed");
					}
				}); // 发送message到topic
			}

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	public static void main(String[] args) {
		CanalKafkaProducerTest test = new CanalKafkaProducerTest();
		test.testProduce();
	}

}
