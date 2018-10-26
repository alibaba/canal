package com.alibaba.otter.canal.adapter.launcher.loader;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.common.errors.WakeupException;

import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.rocketmq.RocketMQCanalConnector;
import com.alibaba.otter.canal.client.rocketmq.RocketMQCanalConnectorProvider;
import com.alibaba.otter.canal.protocol.Message;

/**
 * kafka对应的client适配器工作线程
 *
 * @author rewerma 2018-8-19 下午11:30:49
 * @version 1.0.0
 */
public class CanalAdapterRocketMQWorker extends AbstractCanalAdapterWorker {

    private RocketMQCanalConnector connector;

    private String                 topic;

    public CanalAdapterRocketMQWorker(String nameServers, String topic, String groupId,
                                      List<List<OuterAdapter>> canalOuterAdapters){
        logger.info("RocketMQ consumer config topic:{}, nameServer:{}, groupId:{}", topic, nameServers, groupId);
        this.canalOuterAdapters = canalOuterAdapters;
        this.groupInnerExecutorService = Executors.newFixedThreadPool(canalOuterAdapters.size());
        this.topic = topic;
        this.canalDestination = topic;
        connector = RocketMQCanalConnectorProvider.newRocketMQConnector(nameServers, topic, groupId);
    }

    @Override
    protected void closeConnection() {
        connector.stopRunning();
    }

    @Override
    protected void process() {
        while (!running)
            ;
        ExecutorService executor = Executors.newSingleThreadExecutor();
        while (running) {
            try {
                logger.info("=============> Start to connect topic: {} <=============", this.topic);
                connector.connect();
                logger.info("=============> Start to subscribe topic: {}<=============", this.topic);
                connector.subscribe();
                logger.info("=============> Subscribe topic: {} succeed<=============", this.topic);
                while (running) {
                    try {
                        // switcher.get(); //等待开关开启

                        final Message message = connector.getWithoutAck(1);
                        if (message != null) {
                            executor.submit(() -> {
                                try {
                                    if (logger.isDebugEnabled()) {
                                        logger.debug("topic: {} batchId: {} batchSize: {} ",
                                            topic,
                                            message.getId(),
                                            message.getEntries().size());
                                    }
                                    long begin = System.currentTimeMillis();
                                    writeOut(message);
                                    long now = System.currentTimeMillis();
                                    if ((System.currentTimeMillis() - begin) > 5 * 60 * 1000) {
                                        logger.error("topic: {} batchId {} elapsed time: {} ms",
                                            topic,
                                            message.getId(),
                                            now - begin);
                                    }
                                } catch (Exception e) {
                                    logger.error(e.getMessage(), e);
                                }
                                connector.ack(message.getId());
                            });
                        } else {
                            logger.debug("Message is null");
                        }
                    } catch (CommitFailedException e) {
                        logger.warn(e.getMessage());
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                        TimeUnit.SECONDS.sleep(1L);
                    }
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        executor.shutdown();

        try {
            connector.unsubscribe();
        } catch (WakeupException e) {
            // No-op. Continue process
        }
        connector.stopRunning();
        logger.info("=============> Disconnect topic: {} <=============", this.topic);
    }
}
