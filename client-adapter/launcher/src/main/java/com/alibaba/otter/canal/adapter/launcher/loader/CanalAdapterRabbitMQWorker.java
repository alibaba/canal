package com.alibaba.otter.canal.adapter.launcher.loader;

import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.support.CanalClientConfig;
import com.alibaba.otter.canal.client.adapter.support.Util;
import com.alibaba.otter.canal.client.rabbitmq.RabbitMQCanalConnector;
import org.apache.kafka.common.errors.WakeupException;

import java.util.List;
import java.util.concurrent.ExecutorService;

public class CanalAdapterRabbitMQWorker extends AbstractCanalAdapterWorker {

    private RabbitMQCanalConnector connector;
    private String                 topic;
    private boolean                flatMessage;

    public CanalAdapterRabbitMQWorker(CanalClientConfig canalClientConfig, List<List<OuterAdapter>> canalOuterAdapters,
                                      String topic, String groupId, boolean flatMessage){
        super(canalOuterAdapters);
        this.canalClientConfig = canalClientConfig;
        this.topic = topic;
        this.flatMessage = flatMessage;
        this.canalDestination = topic;
        this.groupId = groupId;
        this.connector = new RabbitMQCanalConnector(canalClientConfig.getMqServers(),
            canalClientConfig.getVhost(),
            topic,
            canalClientConfig.getAccessKey(),
            canalClientConfig.getSecretKey(),
            canalClientConfig.getUsername(),
            canalClientConfig.getPassword(),
            canalClientConfig.getResourceOwnerId(),
            flatMessage);
    }

    @Override
    protected void process() {
        while (!running) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
            }
        }

        ExecutorService workerExecutor = Util.newSingleThreadExecutor(5000L);
        int retry = canalClientConfig.getRetries() == null
                    || canalClientConfig.getRetries() == 0 ? 1 : canalClientConfig.getRetries();
        long timeout = canalClientConfig.getTimeout() == null ? 30000 : canalClientConfig.getTimeout(); // 默认超时30秒
        while (running) {
            try {
                syncSwitch.get(canalDestination);
                logger.info("=============> Start to connect topic: {} <=============", this.topic);
                connector.connect();
                logger.info("=============> Start to subscribe topic: {}<=============", this.topic);
                connector.subscribe();
                logger.info("=============> Subscribe topic: {} succeed<=============", this.topic);
                while (running) {
                    boolean status = syncSwitch.status(canalDestination);
                    if (!status) {
                        connector.disconnect();
                        break;
                    }
                    if (retry == -1) {
                        retry = Integer.MAX_VALUE;
                    }
                    for (int i = 0; i < retry; i++) {
                        if (!running) {
                            break;
                        }
                        if (mqWriteOutData(retry, timeout, i, flatMessage, connector, workerExecutor)) {
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        workerExecutor.shutdown();

        try {
            connector.unsubscribe();
        } catch (WakeupException e) {
            // No-op. Continue process
        }
        connector.disconnect();
        logger.info("=============> Disconnect topic: {} <=============", this.topic);

    }
}
