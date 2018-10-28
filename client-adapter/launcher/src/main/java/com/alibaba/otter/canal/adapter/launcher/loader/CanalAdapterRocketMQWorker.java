package com.alibaba.otter.canal.adapter.launcher.loader;

import com.alibaba.otter.canal.protocol.FlatMessage;
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
 * rocketmq对应的client适配器工作线程
 *
 * @version 1.0.0
 */
public class CanalAdapterRocketMQWorker extends AbstractCanalAdapterWorker {

    private RocketMQCanalConnector connector;

    private String topic;

    private boolean flatMessage;

    public CanalAdapterRocketMQWorker(String nameServers, String topic, String groupId,
        List<List<CanalOuterAdapter>> canalOuterAdapters, boolean flatMessage) {
        logger.info("RocketMQ consumer config topic:{}, nameServer:{}, groupId:{}", topic, nameServers, groupId);
        this.canalOuterAdapters = canalOuterAdapters;
        this.groupInnerExecutorService = Executors.newFixedThreadPool(canalOuterAdapters.size());
        this.topic = topic;
        this.flatMessage = flatMessage;
        this.canalDestination = topic;
        connector = RocketMQCanalConnectorProvider.newRocketMQConnector(nameServers, topic, groupId, flatMessage);
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
                        Object message = null;
                        if (!flatMessage) {
                            message = connector.getWithoutAck(1);
                        } else {
                            message = connector.getFlatMessageWithoutAck();
                        }
                        if (message != null) {
                            final Object msg = message;
                            executor.submit(new Runnable() {
                                @Override
                                public void run() {
                                    try {
                                        if (logger.isDebugEnabled()) {
                                            logger.debug("topic: {} batchId: {} batchSize: {} ",
                                                topic,
                                                message.getId(),
                                                message.getEntries().size());
                                        }
                                        if (msg != null) {
                                            long begin = System.currentTimeMillis();
                                            if (msg instanceof Message) {
                                                Message receive = (Message) msg;
                                                writeOut(receive, topic);
                                                connector.ack(receive.getId());
                                            } else {
                                                FlatMessage receive = (FlatMessage) msg;
                                                writeOut(receive);
                                                connector.ack(receive.getId());
                                            }
                                            long now = System.currentTimeMillis();
                                            if ((now - begin) > 5 * 60 * 1000) {
                                                logger.error("topic: {} batchId {} elapsed time: {} ms",
                                                    topic,
                                                    message.getId(),
                                                    now - begin);
                                            }
                                        }
                                    } catch (Exception e) {
                                        logger.error(e.getMessage(), e);
                                    }
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
