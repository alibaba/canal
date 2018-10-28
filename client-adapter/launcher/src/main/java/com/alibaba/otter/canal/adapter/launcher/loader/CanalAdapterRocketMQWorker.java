package com.alibaba.otter.canal.adapter.launcher.loader;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.errors.WakeupException;

import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.rocketmq.RocketMQCanalConnector;
import com.alibaba.otter.canal.client.rocketmq.RocketMQCanalConnectors;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;

/**
 * rocketmq对应的client适配器工作线程
 *
 * @version 1.0.0
 */
public class CanalAdapterRocketMQWorker extends AbstractCanalAdapterWorker {

    private RocketMQCanalConnector connector;
    private String                 topic;
    private boolean                flatMessage;

    public CanalAdapterRocketMQWorker(String nameServers, String topic, String groupId,
                                      List<List<OuterAdapter>> canalOuterAdapters, boolean flatMessage){
        super(canalOuterAdapters);
        this.topic = topic;
        this.flatMessage = flatMessage;
        this.canalDestination = topic;
        this.connector = RocketMQCanalConnectors.newRocketMQConnector(nameServers, topic, groupId, flatMessage);
        logger.info("RocketMQ consumer config topic:{}, nameServer:{}, groupId:{}", topic, nameServers, groupId);
    }

    @Override
    protected void process() {
        while (!running)
            ;
        while (running) {
            try {
                syncSwitch.get(canalDestination);
                logger.info("=============> Start to connect topic: {} <=============", this.topic);
                connector.connect();
                logger.info("=============> Start to subscribe topic: {}<=============", this.topic);
                connector.subscribe();
                logger.info("=============> Subscribe topic: {} succeed<=============", this.topic);
                while (running) {
                    try {
                        Boolean status = syncSwitch.status(canalDestination);
                        if (status != null && !status) {
                            connector.disconnect();
                            break;
                        }

                        List<?> messages;
                        if (!flatMessage) {
                            messages = connector.getListWithoutAck(100L, TimeUnit.MILLISECONDS);
                        } else {
                            messages = connector.getFlatListWithoutAck(100L, TimeUnit.MILLISECONDS);
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
                    } catch (Throwable e) {
                        logger.error(e.getMessage(), e);
                        TimeUnit.SECONDS.sleep(1L);
                    }
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        try {
            connector.unsubscribe();
        } catch (WakeupException e) {
            // No-op. Continue process
        }
        connector.disconnect();
        logger.info("=============> Disconnect topic: {} <=============", this.topic);
    }
}
