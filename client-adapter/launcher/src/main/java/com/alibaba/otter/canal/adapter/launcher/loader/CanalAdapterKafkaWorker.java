package com.alibaba.otter.canal.adapter.launcher.loader;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.common.errors.WakeupException;

import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.support.CanalClientConfig;
import com.alibaba.otter.canal.client.kafka.KafkaCanalConnector;

/**
 * kafka对应的client适配器工作线程
 *
 * @author rewerma 2018-8-19 下午11:30:49
 * @version 1.0.0
 */
public class CanalAdapterKafkaWorker extends AbstractCanalAdapterWorker {

    private CanalClientConfig   canalClientConfig;
    private KafkaCanalConnector connector;
    private String              topic;
    private boolean             flatMessage;

    public CanalAdapterKafkaWorker(CanalClientConfig canalClientConfig, String bootstrapServers, String topic,
                                   String groupId, List<List<OuterAdapter>> canalOuterAdapters, boolean flatMessage){
        super(canalOuterAdapters);
        this.canalClientConfig = canalClientConfig;
        this.topic = topic;
        this.canalDestination = topic;
        this.flatMessage = flatMessage;
        this.connector = new KafkaCanalConnector(bootstrapServers,
            topic,
            null,
            groupId,
            canalClientConfig.getBatchSize(),
            flatMessage);
        // connector.setSessionTimeout(1L, TimeUnit.MINUTES);
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
                logger.info("=============> Start to subscribe topic: {} <=============", this.topic);
                connector.subscribe();
                logger.info("=============> Subscribe topic: {} succeed <=============", this.topic);
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
