package com.alibaba.otter.canal.adapter.launcher.loader;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.common.errors.WakeupException;

import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.kafka.KafkaCanalConnector;
import com.alibaba.otter.canal.client.kafka.KafkaCanalConnectors;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;

/**
 * kafka对应的client适配器工作线程
 *
 * @author rewerma 2018-8-19 下午11:30:49
 * @version 1.0.0
 */
public class CanalAdapterKafkaWorker extends AbstractCanalAdapterWorker {

    private KafkaCanalConnector connector;

    private String              topic;

    private boolean             flatMessage;

    public CanalAdapterKafkaWorker(String bootstrapServers, String topic, String groupId,
                                   List<List<OuterAdapter>> canalOuterAdapters, boolean flatMessage){
        this.canalOuterAdapters = canalOuterAdapters;
        this.groupInnerExecutorService = Executors.newFixedThreadPool(canalOuterAdapters.size());
        this.topic = topic;
        this.canalDestination = topic;
        this.flatMessage = flatMessage;
        connector = KafkaCanalConnectors.newKafkaConnector(bootstrapServers, topic, null, groupId, flatMessage);
        // connector.setSessionTimeout(1L, TimeUnit.MINUTES);
    }

    @Override
    protected  void closeConnection(){
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
                logger.info("=============> Start to subscribe topic: {} <=============", this.topic);
                connector.subscribe();
                logger.info("=============> Subscribe topic: {} succeed <=============", this.topic);
                while (running) {
                    try {
                        syncSwitch.get(canalDestination);

                        List<?> messages;
                        if (!flatMessage) {
                            messages = connector.getWithoutAck();
                        } else {
                            messages = connector.getFlatMessageWithoutAck(100L, TimeUnit.MILLISECONDS);
                        }
                        if (messages != null) {
                            for (final Object message : messages) {
                                if (message instanceof FlatMessage) {
                                    writeOut((FlatMessage) message);
                                } else {
                                    writeOut((Message) message);
                                }
                            }
                        }
                        connector.ack();
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
        connector.disconnect();
        logger.info("=============> Disconnect topic: {} <=============", this.topic);
    }
}
