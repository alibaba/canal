package com.alibaba.otter.canal.adapter.launcher.loader;

import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.kafka.common.errors.WakeupException;

import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.support.CanalClientConfig;
import com.alibaba.otter.canal.client.adapter.support.Util;
import com.alibaba.otter.canal.client.rocketmq.RocketMQCanalConnector;

/**
 * rocketmq对应的client适配器工作线程
 *
 * @version 1.0.0
 */
public class CanalAdapterRocketMQWorker extends AbstractCanalAdapterWorker {

    private RocketMQCanalConnector connector;
    private String                 topic;
    private boolean                flatMessage;

    public CanalAdapterRocketMQWorker(CanalClientConfig canalClientConfig, String nameServers, String topic,
                                      String groupId, List<List<OuterAdapter>> canalOuterAdapters, String accessKey,
                                      String secretKey, boolean flatMessage){
        super(canalOuterAdapters);
        this.canalClientConfig = canalClientConfig;
        this.topic = topic;
        this.flatMessage = flatMessage;
        super.canalDestination = topic;
        super.groupId = groupId;
        this.connector = new RocketMQCanalConnector(nameServers,
            topic,
            groupId,
            accessKey,
            secretKey,
            canalClientConfig.getBatchSize(),
            flatMessage);
        logger.info("RocketMQ consumer config topic:{}, nameServer:{}, groupId:{}", topic, nameServers, groupId);
    }

    public CanalAdapterRocketMQWorker(CanalClientConfig canalClientConfig, String nameServers, String topic,
        String groupId, List<List<OuterAdapter>> canalOuterAdapters, String accessKey,
        String secretKey, boolean flatMessage, boolean enableMessageTrace,
        String customizedTraceTopic, String accessChannel, String namespace) {
        super(canalOuterAdapters);
        this.canalClientConfig = canalClientConfig;
        this.topic = topic;
        this.flatMessage = flatMessage;
        super.canalDestination = topic;
        super.groupId = groupId;
        this.connector = new RocketMQCanalConnector(nameServers,
            topic,
            groupId,
            accessKey,
            secretKey,
            canalClientConfig.getBatchSize(),
            flatMessage,
            enableMessageTrace,
            customizedTraceTopic,
            accessChannel,
            namespace);
        logger.info("RocketMQ consumer config topic:{}, nameServer:{}, groupId:{}", topic, nameServers, groupId);
    }

    @Override
    protected void process() {
        while (!running) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
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
