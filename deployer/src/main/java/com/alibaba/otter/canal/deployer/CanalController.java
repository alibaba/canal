package com.alibaba.otter.canal.deployer;

import java.util.Map;
import java.util.Properties;

import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.common.zookeeper.ZookeeperPathUtils;
import com.alibaba.otter.canal.common.zookeeper.running.ServerRunningData;
import com.alibaba.otter.canal.common.zookeeper.running.ServerRunningListener;
import com.alibaba.otter.canal.common.zookeeper.running.ServerRunningMonitor;
import com.alibaba.otter.canal.common.zookeeper.running.ServerRunningMonitors;
import com.alibaba.otter.canal.deployer.InstanceConfig.InstanceMode;
import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.instance.core.CanalInstanceGenerator;
import com.alibaba.otter.canal.instance.manager.CanalConfigClient;
import com.alibaba.otter.canal.instance.manager.ManagerCanalInstanceGenerator;
import com.alibaba.otter.canal.instance.spring.SpringCanalInstanceGenerator;
import com.alibaba.otter.canal.server.embeded.CanalServerWithEmbeded;
import com.alibaba.otter.canal.server.exception.CanalServerException;
import com.alibaba.otter.canal.server.netty.CanalServerWithNetty;
import com.google.common.base.Function;
import com.google.common.collect.MapMaker;

/**
 * canal调度控制器
 * 
 * @author jianghang 2012-11-8 下午12:03:11
 * @version 1.0.0
 */
public class CanalController {

    private static final Logger            logger = LoggerFactory.getLogger(CanalController.class);
    private Long                           cid;
    private String                         ip;
    private int                            port;
    // 默认使用spring的方式载入
    private Map<String, InstanceConfig>    instanceConfigs;
    private Map<String, CanalConfigClient> managerClients;
    private CanalServerWithEmbeded         embededCanalServer;
    private CanalServerWithNetty           canalServer;

    private CanalInstanceGenerator         instanceGenerator;
    private ZkClientx                      zkclientx;

    public CanalController(){
        this(System.getProperties());
    }

    public CanalController(Properties properties){
        instanceConfigs = new MapMaker().makeMap();
        managerClients = new MapMaker().makeComputingMap(new Function<String, CanalConfigClient>() {

            public CanalConfigClient apply(String managerAddress) {
                return getManagerClient(managerAddress);
            }
        });

        // 初始化全局参数设置
        initGlobalConfig(properties);

        // 初始化instance config
        initInstanceConfig(properties);

        // 准备canal server
        cid = Long.valueOf(getProperty(properties, CanalConstants.CANAL_ID));
        ip = getProperty(properties, CanalConstants.CANAL_IP);
        if (StringUtils.isEmpty(ip)) {
            ip = AddressUtils.getHostIp();
        }
        port = Integer.valueOf(getProperty(properties, CanalConstants.CANAL_PORT));
        final String zkServers = getProperty(properties, CanalConstants.CANAL_ZKSERVERS);
        if (StringUtils.isNotEmpty(zkServers)) {
            zkclientx = ZkClientx.getZkClient(zkServers);
            final ServerRunningData serverData = new ServerRunningData(cid, ip + ":" + port);
            ServerRunningMonitors.setServerData(serverData);
            ServerRunningMonitors.setRunningMonitors(new MapMaker().makeComputingMap(new Function<String, ServerRunningMonitor>() {

                public ServerRunningMonitor apply(final String destination) {
                    ServerRunningMonitor runningMonitor = new ServerRunningMonitor(serverData);
                    runningMonitor.setDestination(destination);
                    runningMonitor.setListener(new ServerRunningListener() {

                        public void processActiveEnter() {
                            try {
                                MDC.put(CanalConstants.MDC_DESTINATION, String.valueOf(destination));
                                embededCanalServer.start(destination);
                                final String path = ZookeeperPathUtils.getDestinationClusterNode(destination, ip + ":"
                                                                                                              + port);
                                initCid(path);
                                zkclientx.subscribeStateChanges(new IZkStateListener() {

                                    public void handleStateChanged(KeeperState state) throws Exception {

                                    }

                                    public void handleNewSession() throws Exception {
                                        initCid(path);
                                    }
                                });
                            } finally {
                                MDC.remove(CanalConstants.MDC_DESTINATION);
                            }
                        }

                        public void processActiveExit() {
                            try {
                                MDC.put(CanalConstants.MDC_DESTINATION, String.valueOf(destination));
                                final String path = ZookeeperPathUtils.getDestinationClusterNode(destination, ip + ":"
                                                                                                              + port);
                                releaseCid(path);
                                embededCanalServer.stop(destination);
                            } finally {
                                MDC.remove(CanalConstants.MDC_DESTINATION);
                            }
                        }

                    });

                    runningMonitor.setZkClient(ZkClientx.getZkClient(zkServers));
                    return runningMonitor;
                }
            }));

            // 初始化系统目录
            zkclientx.createPersistent(ZookeeperPathUtils.DESTINATION_ROOT_NODE, true);
            zkclientx.createPersistent(ZookeeperPathUtils.CANAL_CLUSTER_ROOT_NODE, true);
        }

        embededCanalServer = new CanalServerWithEmbeded();
        embededCanalServer.setCanalInstanceGenerator(instanceGenerator);// 设置自定义的instanceGenerator
        canalServer = new CanalServerWithNetty(embededCanalServer);
        canalServer.setIp(ip);
        canalServer.setPort(port);
    }

    private void initGlobalConfig(Properties properties) {
        InstanceConfig globalConfig = new InstanceConfig();
        String modeStr = getProperty(properties, CanalConstants.getInstanceModeKey(CanalConstants.GLOBAL_NAME));
        if (StringUtils.isNotEmpty(modeStr)) {
            globalConfig.setMode(InstanceMode.valueOf(StringUtils.upperCase(modeStr)));
        }

        String lazyStr = getProperty(properties, CanalConstants.getInstancLazyKey(CanalConstants.GLOBAL_NAME));
        if (StringUtils.isNotEmpty(lazyStr)) {
            globalConfig.setLazy(Boolean.valueOf(lazyStr));
        }

        String managerAddress = getProperty(properties,
                                            CanalConstants.getInstanceManagerAddressKey(CanalConstants.GLOBAL_NAME));
        if (StringUtils.isNotEmpty(managerAddress)) {
            globalConfig.setManagerAddress(managerAddress);
        }

        String springXml = getProperty(properties, CanalConstants.getInstancSpringXmlKey(CanalConstants.GLOBAL_NAME));
        if (StringUtils.isNotEmpty(springXml)) {
            globalConfig.setSpringRootXml(springXml);
        }
        instanceConfigs.put(CanalConstants.GLOBAL_NAME, globalConfig);

        instanceGenerator = new CanalInstanceGenerator() {

            public CanalInstance generate(String destination) {
                InstanceConfig config = instanceConfigs.get(destination);
                if (config == null) {
                    throw new CanalServerException("can't find destination:{}");
                }

                if (config.getMode().isManager()) {
                    ManagerCanalInstanceGenerator instanceGenerator = new ManagerCanalInstanceGenerator();
                    instanceGenerator.setCanalConfigClient(managerClients.get(config.getManagerAddress()));
                    return instanceGenerator.generate(destination);
                } else if (config.getMode().isSpring()) {
                    SpringCanalInstanceGenerator instanceGenerator = new SpringCanalInstanceGenerator();
                    synchronized (this) {
                        try {
                            // 设置当前正在加载的通道，加载spring查找文件时会用到该变量
                            System.setProperty(CanalConstants.CANAL_DESTINATION_PROPERTY, destination);
                            instanceGenerator.setBeanFactory(getBeanFactory(config.getSpringRootXml()));
                            return instanceGenerator.generate(destination);
                        } finally {
                            System.setProperty(CanalConstants.CANAL_DESTINATION_PROPERTY, "");
                        }
                    }
                } else {
                    throw new UnsupportedOperationException("unknow mode :" + config.getMode());
                }

            }

        };
    }

    private CanalConfigClient getManagerClient(String managerAddress) {
        return new CanalConfigClient();
    }

    private BeanFactory getBeanFactory(String springXml) {
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext(springXml);
        return applicationContext;
    }

    private void initInstanceConfig(Properties properties) {
        String destinationStr = getProperty(properties, CanalConstants.CANAL_DESTINATIONS);
        String[] destinations = StringUtils.split(destinationStr, CanalConstants.CANAL_DESTINATION_SPLIT);
        for (String destination : destinations) {
            InstanceConfig config = parseInstanceConfig(properties, destination);
            InstanceConfig oldConfig = instanceConfigs.put(destination, config);

            if (oldConfig != null) {
                logger.warn("destination:{} old config:{} has replace by new config:{}", new Object[] { destination,
                        oldConfig, config });
            }
        }
    }

    private InstanceConfig parseInstanceConfig(Properties properties, String destination) {
        InstanceConfig config = new InstanceConfig(instanceConfigs.get(CanalConstants.GLOBAL_NAME));
        String modeStr = getProperty(properties, CanalConstants.getInstanceModeKey(destination));
        if (!StringUtils.isEmpty(modeStr)) {
            config.setMode(InstanceMode.valueOf(StringUtils.upperCase(modeStr)));
        }

        String lazyStr = getProperty(properties, CanalConstants.getInstancLazyKey(destination));
        if (!StringUtils.isEmpty(lazyStr)) {
            config.setLazy(Boolean.valueOf(lazyStr));
        }

        if (config.getMode().isManager()) {
            String managerAddress = getProperty(properties, CanalConstants.getInstanceManagerAddressKey(destination));
            if (StringUtils.isNotEmpty(managerAddress)) {
                config.setManagerAddress(managerAddress);
            }
        } else if (config.getMode().isSpring()) {
            String springXml = getProperty(properties, CanalConstants.getInstancSpringXmlKey(destination));
            if (StringUtils.isNotEmpty(springXml)) {
                config.setSpringCustomXml(springXml);
            }
        }

        return config;
    }

    private String getProperty(Properties properties, String key) {
        return StringUtils.trim(properties.getProperty(StringUtils.trim(key)));
    }

    public void start() throws Throwable {
        logger.info("## start the canal server[{}:{}]", ip, port);
        // 创建整个canal的工作节点
        final String path = ZookeeperPathUtils.getCanalClusterNode(ip + ":" + port);
        initCid(path);
        if (zkclientx != null) {
            this.zkclientx.subscribeStateChanges(new IZkStateListener() {

                public void handleStateChanged(KeeperState state) throws Exception {

                }

                public void handleNewSession() throws Exception {
                    initCid(path);
                }
            });
        }
        // 启动网络接口
        canalServer.start();
        // 尝试启动一下非lazy状态的通道
        for (Map.Entry<String, InstanceConfig> entry : instanceConfigs.entrySet()) {
            final String destination = entry.getKey();
            InstanceConfig config = entry.getValue();
            if (!StringUtils.equalsIgnoreCase(destination, CanalConstants.GLOBAL_NAME)) {
                // 创建destination的工作节点
                if (!config.getLazy() && !embededCanalServer.isStart(destination)) {
                    if (zkclientx != null) {
                        // HA机制启动
                        ServerRunningMonitor runningMonitor = ServerRunningMonitors.getRunningMonitor(destination);
                        if (!runningMonitor.isStart()) {
                            runningMonitor.start();
                        }
                    } else {
                        // 普通模式启动
                        try {
                            MDC.put(CanalConstants.MDC_DESTINATION, String.valueOf(destination));
                            embededCanalServer.start(destination);
                        } finally {
                            MDC.remove(CanalConstants.MDC_DESTINATION);
                        }
                    }
                }

            }
        }
    }

    public void stop() throws Throwable {
        canalServer.stop();

        if (zkclientx != null) {
            for (ServerRunningMonitor runningMonitor : ServerRunningMonitors.getRunningMonitors().values()) {
                if (runningMonitor.isStart()) {
                    runningMonitor.stop();
                }
            }
        }

        // 释放canal的工作节点
        releaseCid(ZookeeperPathUtils.getCanalClusterNode(ip + ":" + port));
        logger.info("## stop the canal server[{}:{}]", ip, port);
    }

    private void initCid(String path) {
        // logger.info("## init the canalId = {}", cid);
        // 初始化系统目录
        if (zkclientx != null) {
            try {
                zkclientx.createEphemeral(path);
            } catch (ZkNoNodeException e) {
                // 如果父目录不存在，则创建
                String parentDir = path.substring(0, path.lastIndexOf('/'));
                zkclientx.createPersistent(parentDir, true);
                zkclientx.createEphemeral(path);
            }

        }
    }

    private void releaseCid(String path) {
        // logger.info("## release the canalId = {}", cid);
        // 初始化系统目录
        if (zkclientx != null) {
            zkclientx.delete(path);
        }
    }

}
