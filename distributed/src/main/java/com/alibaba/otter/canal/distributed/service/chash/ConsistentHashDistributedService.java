package com.alibaba.otter.canal.distributed.service.chash;

import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.distributed.service.DistributedService;
import com.alibaba.otter.canal.distributed.service.ExecutionService;
import com.alibaba.otter.canal.distributed.service.MonitorService;
import com.alibaba.otter.canal.distributed.service.chash.util.ConsistentHash;
import com.alibaba.otter.canal.distributed.service.chash.util.HashService;
import com.alibaba.otter.canal.distributed.service.zkmonitor.ZookeeperMonitorService;

/**
 * 一致性hash+zookeeper实现分布式服务
 *
 * @author rewerma 2020-11-8 下午03:07:11
 * @version 1.0.0
 */
public class ConsistentHashDistributedService implements DistributedService {

    private static final Logger   logger           = LoggerFactory.getLogger(ConsistentHashDistributedService.class);

    private MonitorService        monitorService;                                                                    // 节点实监控器服务
    private Set<String>           instances        = new CopyOnWriteArraySet<>();                                    // 实例列表
    private volatile List<String> nodes            = null;                                                           // 节点列表
    private String                currentNode;                                                                       // 当前节点
    private Set<String>           currentInstances = new CopyOnWriteArraySet<>();                                    // 当前节点运行的实例列表

    private ExecutionService      executionService;

    public ConsistentHashDistributedService(ZkClientx zkClientx, Collection<String> instances,
                                            ExecutionService executionService){
        this.monitorService = new ZookeeperMonitorService(zkClientx); // ZK实现
        this.instances.addAll(instances);
        this.executionService = executionService;
    }

    @Override
    public synchronized void registerNode(String nodeName) {
        this.currentNode = nodeName;

        monitorService.init(nodeName);

        ConsistentHashDistributedService lock = this;
        if (nodes == null) {
            nodes = monitorService.listen(nodeList -> {
                synchronized (lock) {
                    logger.info("Node changed, left node: {}", nodeList);
                    nodes = nodeList;
                    distribute();
                }
            });
        }
    }

    @Override
    public synchronized void releaseNode() {
        monitorService.destroy();
    }

    @Override
    public synchronized void distribute() {
        if (instances != null && nodes != null) {
            try {
                Map<String, List<String>> map = new HashMap<>();

                ConsistentHash<String> consistentHash = new ConsistentHash<>(new HashService(), 5000, nodes);

                for (String instance : instances) {
                    String node = consistentHash.get(instance); // 为实例分配一个节点
                    if (node == null) {
                        // 无节点可以分配
                        logger.warn("No node for distribute: {}", instance);
                        continue;
                    }
                    List<String> instancesSub = map.computeIfAbsent(node, k -> new ArrayList<>());
                    instancesSub.add(instance);
                }
                List<String> instancesSub = map.get(this.currentNode);
                if (instancesSub == null) {
                    instancesSub = new ArrayList<>();
                }

                List<String> currentInstancesTmp = new ArrayList<>(currentInstances); // 当前节点所运行的实例
                currentInstancesTmp.removeAll(instancesSub); // 需要停止的实例

                List<String> instancesSubTmp = new ArrayList<>(instancesSub); // 当前节点所分配的实例
                instancesSubTmp.removeAll(currentInstances); // 需要启动的实例

                currentInstances.clear();
                currentInstances.addAll(instancesSub); // 设置当前节点运行的实例

                for (String instance : currentInstancesTmp) {
                    executionService.stop(instance); // 停止删除的实例
                }
                Thread.sleep(3000L); // 延时一段时间确保其它进程的停止操作已完成
                for (String instance : instancesSubTmp) {
                    executionService.start(instance); // 启动新增的实例
                }

            } catch (Throwable e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    @Override
    public synchronized boolean addInstance(String instance) {
        instances.add(instance);
        distribute();
        return containInstance(instance);
    }

    @Override
    public synchronized void removeInstance(String instance) {
        instances.remove(instance);
        distribute();
    }

    @Override
    public synchronized boolean containInstance(String instance) {
        return currentInstances.contains(instance);
    }

    @Override
    public synchronized boolean removeInstanceFromCurrentNode(String instance) {
        return currentInstances.remove(instance);
    }
}
