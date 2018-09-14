package com.alibaba.otter.canal.client.adapter.loader;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.adapter.CanalOuterAdapter;
import com.alibaba.otter.canal.client.adapter.support.CanalClientConfig;
import com.alibaba.otter.canal.client.adapter.support.CanalOuterAdapterConfiguration;
import com.alibaba.otter.canal.client.adapter.support.ExtensionLoader;

/**
 * 外部适配器的加载器
 *
 * @author machengyuan 2018-8-19 下午11:45:49
 * @version 1.0.0
 */
public class CanalAdapterLoader {

    private static final Logger                  logger            = LoggerFactory.getLogger(CanalAdapterLoader.class);

    private CanalClientConfig                    canalClientConfig;

    private Map<String, CanalAdapterWorker>      canalWorkers      = new HashMap<>();

    private Map<String, CanalAdapterKafkaWorker> canalKafkaWorkers = new HashMap<>();

    private ExtensionLoader<CanalOuterAdapter>   loader;

    public CanalAdapterLoader(CanalClientConfig canalClientConfig){
        this.canalClientConfig = canalClientConfig;
    }

    /**
     * 初始化canal-client、 canal-client-kafka的适配器
     */
    public void init() {
        // canal instances 和 kafka topics 配置不能同时为空
        if (canalClientConfig.getCanalInstances() == null && canalClientConfig.getKafkaTopics() == null) {
            throw new RuntimeException("Blank config property: canalInstances or canalKafkaTopics");
        }

        loader = ExtensionLoader.getExtensionLoader(CanalOuterAdapter.class,
            "" /* TODO canalClientConfig.getClassloaderPolicy() */);

        String canalServerHost = this.canalClientConfig.getCanalServerHost();
        SocketAddress sa = null;
        if (canalServerHost != null) {
            String[] ipPort = canalServerHost.split(":");
            sa = new InetSocketAddress(ipPort[0], Integer.parseInt(ipPort[1]));
        }
        String zkHosts = this.canalClientConfig.getZookeeperHosts();

        boolean flatMessage = this.canalClientConfig.getFlatMessage();

        // if (zkHosts == null && sa == null) {
        // throw new RuntimeException("Blank config property: canalServerHost or
        // zookeeperHosts");
        // }

        // 初始化canal-client的适配器
        if (canalClientConfig.getCanalInstances() != null) {
            for (CanalClientConfig.CanalInstance instance : canalClientConfig.getCanalInstances()) {
                List<List<CanalOuterAdapter>> canalOuterAdapterGroups = new ArrayList<>();

                for (CanalClientConfig.AdapterGroup connectorGroup : instance.getAdapterGroups()) {
                    List<CanalOuterAdapter> canalOutConnectors = new ArrayList<>();
                    for (CanalOuterAdapterConfiguration c : connectorGroup.getOutAdapters()) {
                        loadConnector(c, canalOutConnectors);
                    }
                    canalOuterAdapterGroups.add(canalOutConnectors);
                }
                CanalAdapterWorker worker;
                if (zkHosts != null) {
                    worker = new CanalAdapterWorker(instance.getInstance(), zkHosts, canalOuterAdapterGroups);
                } else {
                    worker = new CanalAdapterWorker(instance.getInstance(), sa, canalOuterAdapterGroups);
                }
                canalWorkers.put(instance.getInstance(), worker);
                worker.start();
                logger.info("Start adapter for canal instance: {} succeed", instance.getInstance());
            }
        }

        // 初始化canal-client-kafka的适配器
        if (canalClientConfig.getKafkaTopics() != null) {
            for (CanalClientConfig.KafkaTopic kafkaTopic : canalClientConfig.getKafkaTopics()) {
                for (CanalClientConfig.Group group : kafkaTopic.getGroups()) {
                    List<List<CanalOuterAdapter>> canalOuterAdapterGroups = new ArrayList<>();

                    List<CanalOuterAdapter> canalOuterAdapters = new ArrayList<>();

                    for (CanalOuterAdapterConfiguration config : group.getOutAdapters()) {
                        // for (CanalOuterAdapterConfiguration config : adaptor.getOutAdapters()) {
                        loadConnector(config, canalOuterAdapters);
                        // }
                    }
                    canalOuterAdapterGroups.add(canalOuterAdapters);

                    // String zkServers = canalClientConfig.getZookeeperHosts();
                    CanalAdapterKafkaWorker canalKafkaWorker = new CanalAdapterKafkaWorker(
                        canalClientConfig.getBootstrapServers(),
                        kafkaTopic.getTopic(),
                        group.getGroupId(),
                        canalOuterAdapterGroups,
                        flatMessage);
                    canalKafkaWorkers.put(kafkaTopic.getTopic() + "-" + group.getGroupId(), canalKafkaWorker);
                    canalKafkaWorker.start();
                    logger.info("Start adapter for canal-client kafka topic: {} succeed",
                        kafkaTopic.getTopic() + "-" + group.getGroupId());
                }
            }
        }
    }

    private void loadConnector(CanalOuterAdapterConfiguration config, List<CanalOuterAdapter> canalOutConnectors) {
        try {
            CanalOuterAdapter adapter = loader.getExtension(config.getName());
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
            for (CanalAdapterWorker v : canalWorkers.values()) {
                final CanalAdapterWorker caw = v;
                stopExecutorService.submit(new Runnable() {

                    @Override
                    public void run() {
                        caw.stop();
                    }
                });
            }
            stopExecutorService.shutdown();
        }
        if (canalKafkaWorkers.size() > 0) {
            ExecutorService stopKafkaExecutorService = Executors.newFixedThreadPool(canalKafkaWorkers.size());
            for (CanalAdapterKafkaWorker v : canalKafkaWorkers.values()) {
                final CanalAdapterKafkaWorker cakw = v;
                stopKafkaExecutorService.submit(new Runnable() {

                    @Override
                    public void run() {
                        cakw.stop();
                    }
                });
            }
            stopKafkaExecutorService.shutdown();
        }
        logger.info("All canal adapters destroyed");
    }
}
