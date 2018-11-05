package com.alibaba.otter.canal.adapter.launcher.loader;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.support.CanalClientConfig;
import com.alibaba.otter.canal.client.adapter.support.ExtensionLoader;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;

/**
 * 外部适配器的加载器
 *
 * @version 1.0.0
 */
public class CanalAdapterLoader {

    private static final Logger                     logger        = LoggerFactory.getLogger(CanalAdapterLoader.class);

    private CanalClientConfig                       canalClientConfig;

    private Map<String, CanalAdapterWorker>         canalWorkers  = new HashMap<>();

    private Map<String, AbstractCanalAdapterWorker> canalMQWorker = new HashMap<>();

    private ExtensionLoader<OuterAdapter>           loader;

    public CanalAdapterLoader(CanalClientConfig canalClientConfig){
        this.canalClientConfig = canalClientConfig;
    }

    /**
     * 初始化canal-client
     */
    public void init() {
        loader = ExtensionLoader.getExtensionLoader(OuterAdapter.class);

        String canalServerHost = this.canalClientConfig.getCanalServerHost();
        SocketAddress sa = null;
        if (canalServerHost != null) {
            String[] ipPort = canalServerHost.split(":");
            sa = new InetSocketAddress(ipPort[0], Integer.parseInt(ipPort[1]));
        }
        String zkHosts = this.canalClientConfig.getZookeeperHosts();

        // 初始化canal-client的适配器
        if (canalClientConfig.getCanalInstances() != null) {
            for (CanalClientConfig.CanalInstance instance : canalClientConfig.getCanalInstances()) {
                List<List<OuterAdapter>> canalOuterAdapterGroups = new ArrayList<>();

                for (CanalClientConfig.Group connectorGroup : instance.getGroups()) {
                    List<OuterAdapter> canalOutConnectors = new ArrayList<>();
                    for (OuterAdapterConfig c : connectorGroup.getOutAdapters()) {
                        loadConnector(c, canalOutConnectors);
                    }
                    canalOuterAdapterGroups.add(canalOutConnectors);
                }
                CanalAdapterWorker worker;
                if (sa != null) {
                    worker = new CanalAdapterWorker(canalClientConfig,
                        instance.getInstance(),
                        sa,
                        canalOuterAdapterGroups);
                } else if (zkHosts != null) {
                    worker = new CanalAdapterWorker(canalClientConfig,
                        instance.getInstance(),
                        zkHosts,
                        canalOuterAdapterGroups);
                } else {
                    throw new RuntimeException("No canal server connector found");
                }
                canalWorkers.put(instance.getInstance(), worker);
                worker.start();
                logger.info("Start adapter for canal instance: {} succeed", instance.getInstance());
            }
        }

        // 初始化canal-client-mq的适配器
        if (canalClientConfig.getMqTopics() != null) {
            for (CanalClientConfig.MQTopic topic : canalClientConfig.getMqTopics()) {
                for (CanalClientConfig.MQGroup group : topic.getGroups()) {
                    List<List<OuterAdapter>> canalOuterAdapterGroups = new ArrayList<>();
                    List<OuterAdapter> canalOuterAdapters = new ArrayList<>();
                    for (OuterAdapterConfig config : group.getOutAdapters()) {
                        loadConnector(config, canalOuterAdapters);
                    }
                    canalOuterAdapterGroups.add(canalOuterAdapters);
                    if (StringUtils.isBlank(topic.getMqMode()) || "rocketmq".equalsIgnoreCase(topic.getMqMode())) {
                        CanalAdapterRocketMQWorker rocketMQWorker = new CanalAdapterRocketMQWorker(canalClientConfig,
                            canalClientConfig.getBootstrapServers(),
                            topic.getTopic(),
                            group.getGroupId(),
                            canalOuterAdapterGroups,
                            canalClientConfig.getFlatMessage());
                        canalMQWorker.put(topic.getTopic() + "-rocketmq-" + group.getGroupId(), rocketMQWorker);
                        rocketMQWorker.start();
                    } else if ("kafka".equalsIgnoreCase(topic.getMqMode())) {
                        CanalAdapterKafkaWorker canalKafkaWorker = new CanalAdapterKafkaWorker(canalClientConfig,
                            canalClientConfig.getBootstrapServers(),
                            topic.getTopic(),
                            group.getGroupId(),
                            canalOuterAdapterGroups,
                            canalClientConfig.getFlatMessage());
                        canalMQWorker.put(topic.getTopic() + "-kafka-" + group.getGroupId(), canalKafkaWorker);
                        canalKafkaWorker.start();
                    }
                    logger.info("Start adapter for canal-client mq topic: {} succeed",
                        topic.getTopic() + "-" + group.getGroupId());
                }
            }
        }
    }

    private void loadConnector(OuterAdapterConfig config, List<OuterAdapter> canalOutConnectors) {
        try {
            OuterAdapter adapter = loader.getExtension(config.getName());
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            // 替换ClassLoader
            Thread.currentThread().setContextClassLoader(adapter.getClass().getClassLoader());
            adapter.init(config);
            Thread.currentThread().setContextClassLoader(cl);
            canalOutConnectors.add(adapter);
            logger.info("Load canal adapter: {} succeed", config.getName());
        } catch (Exception e) {
            logger.error("Load canal adapter: {} failed", config.getName(), e);
        }
    }

    /**
     * 销毁所有适配器 为防止canal实例太多造成销毁阻塞, 并行销毁
     */
    public void destroy() {
        if (canalWorkers.size() > 0) {
            ExecutorService stopExecutorService = Executors.newFixedThreadPool(canalWorkers.size());
            for (CanalAdapterWorker canalAdapterWorker : canalWorkers.values()) {
                stopExecutorService.submit(canalAdapterWorker::stop);
            }
            stopExecutorService.shutdown();
        }

        if (canalMQWorker.size() > 0) {
            ExecutorService stopMQWorkerService = Executors.newFixedThreadPool(canalMQWorker.size());
            for (AbstractCanalAdapterWorker canalAdapterMQWorker : canalMQWorker.values()) {
                stopMQWorkerService.submit(canalAdapterMQWorker::stop);
            }
            stopMQWorkerService.shutdown();
        }
        logger.info("All canal adapters destroyed");
    }
}
