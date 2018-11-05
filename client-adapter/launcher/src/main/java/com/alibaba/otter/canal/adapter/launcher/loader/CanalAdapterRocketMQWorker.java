package com.alibaba.otter.canal.adapter.launcher.loader;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.alibaba.otter.canal.client.adapter.support.CanalClientConfig;
import org.apache.kafka.common.errors.WakeupException;

import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.rocketmq.RocketMQCanalConnector;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;

/**
 * rocketmq对应的client适配器工作线程
 *
 * @version 1.0.0
 */
public class CanalAdapterRocketMQWorker extends AbstractCanalAdapterWorker {

    private CanalClientConfig      canalClientConfig;
    private RocketMQCanalConnector connector;
    private String                 topic;
    private boolean                flatMessage;

    public CanalAdapterRocketMQWorker(CanalClientConfig canalClientConfig, String nameServers, String topic,
                                      String groupId, List<List<OuterAdapter>> canalOuterAdapters, boolean flatMessage){
        super(canalOuterAdapters);
        this.canalClientConfig = canalClientConfig;
        this.topic = topic;
        this.flatMessage = flatMessage;
        this.canalDestination = topic;
        this.connector = new RocketMQCanalConnector(nameServers, topic, groupId, flatMessage);
        logger.info("RocketMQ consumer config topic:{}, nameServer:{}, groupId:{}", topic, nameServers, groupId);
    }

    @Override
    protected void process() {
        while (!running)
            ;

        ExecutorService workerExecutor = Executors.newSingleThreadExecutor();
        int retry = canalClientConfig.getRetry() == null ? 1 : canalClientConfig.getRetry();
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
                    Boolean status = syncSwitch.status(canalDestination);
                    if (status != null && !status) {
                        connector.disconnect();
                        break;
                    }
                    mqWriteOutData(retry, timeout, flatMessage, connector, workerExecutor);
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
