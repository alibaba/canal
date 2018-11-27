package com.alibaba.otter.canal.adapter.launcher.loader;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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

        if ("tcp".equalsIgnoreCase(canalClientConfig.getMode())) {
            // 初始化canal-client的适配器
            for (CanalClientConfig.CanalAdapter canalAdapter : canalClientConfig.getCanalAdapters()) {
                List<List<OuterAdapter>> canalOuterAdapterGroups = new ArrayList<>();

                for (CanalClientConfig.Group connectorGroup : canalAdapter.getGroups()) {
                    List<OuterAdapter> canalOutConnectors = new ArrayList<>();
                    for (OuterAdapterConfig c : connectorGroup.getOuterAdapters()) {
                        loadConnector(c, canalOutConnectors);
                    }
                    canalOuterAdapterGroups.add(canalOutConnectors);
                }
                CanalAdapterWorker worker;
                if (sa != null) {
                    worker = new CanalAdapterWorker(canalClientConfig,
                        canalAdapter.getInstance(),
                        sa,
                        canalOuterAdapterGroups);
                } else if (zkHosts != null) {
                    worker = new CanalAdapterWorker(canalClientConfig,
                        canalAdapter.getInstance(),
                        zkHosts,
                        canalOuterAdapterGroups);
                } else {
                    throw new RuntimeException("No canal server connector found");
                }
                canalWorkers.put(canalAdapter.getInstance(), worker);
                worker.start();
                logger.info("Start adapter for canal instance: {} succeed", canalAdapter.getInstance());
            }
        } else if ("kafka".equalsIgnoreCase(canalClientConfig.getMode())) {
            // 初始化canal-client-kafka的适配器
            for (CanalClientConfig.CanalAdapter canalAdapter : canalClientConfig.getCanalAdapters()) {
                for (CanalClientConfig.Group group : canalAdapter.getGroups()) {
                    List<List<OuterAdapter>> canalOuterAdapterGroups = new ArrayList<>();
                    List<OuterAdapter> canalOuterAdapters = new ArrayList<>();
                    for (OuterAdapterConfig config : group.getOuterAdapters()) {
                        loadConnector(config, canalOuterAdapters);
                    }
                    canalOuterAdapterGroups.add(canalOuterAdapters);

                    CanalAdapterKafkaWorker canalKafkaWorker = new CanalAdapterKafkaWorker(canalClientConfig,
                        canalClientConfig.getMqServers(),
                        canalAdapter.getInstance(),
                        group.getGroupId(),
                        canalOuterAdapterGroups,
                        canalClientConfig.getFlatMessage());
                    canalMQWorker.put(canalAdapter.getInstance() + "-kafka-" + group.getGroupId(), canalKafkaWorker);
                    canalKafkaWorker.start();
                    logger.info("Start adapter for canal-client mq topic: {} succeed", canalAdapter.getInstance() + "-"
                                                                                       + group.getGroupId());
                }
            }
        } else if ("rocketMQ".equalsIgnoreCase(canalClientConfig.getMode())) {
            // 初始化canal-client-rocketMQ的适配器
            for (CanalClientConfig.CanalAdapter canalAdapter : canalClientConfig.getCanalAdapters()) {
                for (CanalClientConfig.Group group : canalAdapter.getGroups()) {
                    List<List<OuterAdapter>> canalOuterAdapterGroups = new ArrayList<>();
                    List<OuterAdapter> canalOuterAdapters = new ArrayList<>();
                    for (OuterAdapterConfig config : group.getOuterAdapters()) {
                        loadConnector(config, canalOuterAdapters);
                    }
                    canalOuterAdapterGroups.add(canalOuterAdapters);
                    CanalAdapterRocketMQWorker rocketMQWorker = new CanalAdapterRocketMQWorker(canalClientConfig,
                        canalClientConfig.getMqServers(),
                        canalAdapter.getInstance(),
                        group.getGroupId(),
                        canalOuterAdapterGroups,
                        canalClientConfig.getAccessKey(),
                        canalClientConfig.getSecretKey(),
                        canalClientConfig.getFlatMessage());
                    canalMQWorker.put(canalAdapter.getInstance() + "-rocketmq-" + group.getGroupId(), rocketMQWorker);
                    rocketMQWorker.start();

                    logger.info("Start adapter for canal-client mq topic: {} succeed", canalAdapter.getInstance() + "-"
                                                                                       + group.getGroupId());
                }
            }
        }
    }

    private void loadConnector(OuterAdapterConfig config, List<OuterAdapter> canalOutConnectors) {
        try {
            OuterAdapter adapter;
            adapter = loader.getExtension(config.getName(), StringUtils.trimToEmpty(config.getKey()));

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
        if (!canalWorkers.isEmpty()) {
            ExecutorService stopExecutorService = Executors.newFixedThreadPool(canalWorkers.size());
            List<Future<Boolean>> futures = new ArrayList<>();
            for (CanalAdapterWorker canalAdapterWorker : canalWorkers.values()) {
                futures.add(stopExecutorService.submit(() -> {
                    canalAdapterWorker.stop();
                    return true;
                }));
            }
            futures.forEach(future -> {
                try {
                    future.get();
                } catch (Exception e) {
                    // ignore
                }
            });
            stopExecutorService.shutdown();
        }

        if (!canalMQWorker.isEmpty()) {
            ExecutorService stopMQWorkerService = Executors.newFixedThreadPool(canalMQWorker.size());
            List<Future<Boolean>> futures = new ArrayList<>();
            for (AbstractCanalAdapterWorker canalAdapterMQWorker : canalMQWorker.values()) {
                futures.add(stopMQWorkerService.submit(() -> {
                    canalAdapterMQWorker.stop();
                    return true;
                }));
            }
            futures.forEach(future -> {
                try {
                    future.get();
                } catch (Exception e) {
                    // ignore
                }
            });
            stopMQWorkerService.shutdown();
        }
        logger.info("All canal adapters destroyed");
    }
}
