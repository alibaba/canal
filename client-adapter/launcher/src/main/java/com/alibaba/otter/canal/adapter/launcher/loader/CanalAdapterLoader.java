package com.alibaba.otter.canal.adapter.launcher.loader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.StandardEnvironment;

import com.alibaba.otter.canal.adapter.launcher.config.SpringContext;
import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.support.CanalClientConfig;
import com.alibaba.otter.canal.client.adapter.support.ExtensionLoader;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;
import com.alibaba.otter.canal.client.adapter.support.Util;

/**
 * 外部适配器的加载器
 *
 * @version 1.0.0
 */
public class CanalAdapterLoader {

    private static final Logger           logger                 = LoggerFactory.getLogger(CanalAdapterLoader.class);

    private CanalClientConfig             canalClientConfig;

    private Map<String, AdapterProcessor> canalAdapterProcessors = new HashMap<>();

    private ExtensionLoader<OuterAdapter> loader;

    public CanalAdapterLoader(CanalClientConfig canalClientConfig){
        this.canalClientConfig = canalClientConfig;
    }

    /**
     * 初始化canal-client
     */
    public void init() {
        loader = ExtensionLoader.getExtensionLoader(OuterAdapter.class);

        for (CanalClientConfig.CanalAdapter canalAdapter : canalClientConfig.getCanalAdapters()) {
            for (CanalClientConfig.Group group : canalAdapter.getGroups()) {
                int autoGenId = 0;
                List<List<OuterAdapter>> canalOuterAdapterGroups = new CopyOnWriteArrayList<>();
                List<OuterAdapter> canalOuterAdapters = new CopyOnWriteArrayList<>();

                for (OuterAdapterConfig config : group.getOuterAdapters()) {
                    // 保证一定有key
                    if (StringUtils.isEmpty(config.getKey())) {
                        String key = StringUtils.join(
                            new String[] { Util.AUTO_GENERATED_PREFIX, canalAdapter.getInstance(), group.getGroupId(),
                                           String.valueOf(autoGenId) },
                            '-');
                        //gen keyId
                        config.setKey(key);
                    }
                    autoGenId++;
                    loadAdapter(config, canalOuterAdapters);
                }
                canalOuterAdapterGroups.add(canalOuterAdapters);

                AdapterProcessor adapterProcessor = canalAdapterProcessors.computeIfAbsent(
                    canalAdapter.getInstance() + "|" + StringUtils.trimToEmpty(group.getGroupId()),
                    f -> new AdapterProcessor(canalClientConfig,
                        canalAdapter.getInstance(),
                        group.getGroupId(),
                        canalOuterAdapterGroups));
                adapterProcessor.start();

                logger.info("Start adapter for canal-client mq topic: {} succeed",
                    canalAdapter.getInstance() + "-" + group.getGroupId());
            }
        }
    }

    private void loadAdapter(OuterAdapterConfig config, List<OuterAdapter> canalOutConnectors) {
        try {
            OuterAdapter adapter;
            adapter = loader.getExtension(config.getName(), config.getKey());

            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            // 替换ClassLoader
            Thread.currentThread().setContextClassLoader(adapter.getClass().getClassLoader());
            Environment env = (Environment) SpringContext.getBean(Environment.class);
            Properties evnProperties = null;
            if (env instanceof StandardEnvironment) {
                evnProperties = new Properties();
                for (PropertySource<?> propertySource : ((StandardEnvironment) env).getPropertySources()) {
                    if (propertySource instanceof EnumerablePropertySource) {
                        String[] names = ((EnumerablePropertySource<?>) propertySource).getPropertyNames();
                        for (String name : names) {
                            Object val = env.getProperty(name);
                            if (val != null) {
                                evnProperties.put(name, val);
                            }
                        }
                    }
                }
            }
            adapter.init(config, evnProperties);
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
        if (!canalAdapterProcessors.isEmpty()) {
            ExecutorService stopExecutorService = Executors.newFixedThreadPool(canalAdapterProcessors.size());
            for (AdapterProcessor adapterProcessor : canalAdapterProcessors.values()) {
                stopExecutorService.execute(adapterProcessor::stop);
            }
            stopExecutorService.shutdown();
            try {
                while (!stopExecutorService.awaitTermination(1, TimeUnit.SECONDS)) {
                    // ignore
                }
            } catch (InterruptedException e) {
                // ignore
            }
        }
        logger.info("All canal adapters destroyed");
    }
}
