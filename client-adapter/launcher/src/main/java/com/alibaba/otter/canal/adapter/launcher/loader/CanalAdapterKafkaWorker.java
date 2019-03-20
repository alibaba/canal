package com.alibaba.otter.canal.adapter.launcher.loader;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.errors.WakeupException;

import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.support.CanalClientConfig;
import com.alibaba.otter.canal.client.adapter.support.Util;
import com.alibaba.otter.canal.client.kafka.KafkaCanalConnector;

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

    public CanalAdapterKafkaWorker(CanalClientConfig canalClientConfig, String bootstrapServers, String topic,
                                   String groupId, List<List<OuterAdapter>> canalOuterAdapters, boolean flatMessage){
        super(canalOuterAdapters);
        this.canalClientConfig = canalClientConfig;
        this.topic = topic;
        super.canalDestination = topic;
        super.groupId = groupId;
        this.flatMessage = flatMessage;
        this.connector = new KafkaCanalConnector(bootstrapServers,
            topic,
            null,
            groupId,
            canalClientConfig.getBatchSize(),
            flatMessage);
        connector.setSessionTimeout(30L, TimeUnit.SECONDS);
    }

    @Override
    protected void process() {
        while (!running) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                // ignore
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
                logger.info("=============> Start to subscribe topic: {} <=============", this.topic);
                connector.subscribe();
                logger.info("=============> Subscribe topic: {} succeed <=============", this.topic);
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
