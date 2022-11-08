package com.alibaba.otter.canal.meta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.alibaba.otter.canal.meta.exception.CanalMetaManagerException;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.protocol.position.PositionRange;
import com.google.common.collect.MigrateMap;

/**
 * 基于定时刷新的策略的mixed实现
 * 
 * <pre>
 * 几个优化：
 * 1. 去除batch数据刷新到zk中，切换时batch数据可忽略，重新从头开始获取
 * 2. cursor的更新，启用定时刷新，合并多次请求。如果最近没有变化则不更新
 * </pre>
 * 
 * @author jianghang 2012-9-11 下午02:41:15
 * @version 1.0.0
 */
public class PeriodMixedMetaManager extends MemoryMetaManager implements CanalMetaManager {

    private static final Logger      logger     = LoggerFactory.getLogger(PeriodMixedMetaManager.class);
    private ScheduledExecutorService executor;
    private ZooKeeperMetaManager     zooKeeperMetaManager;
    @SuppressWarnings("serial")
    private final Position           nullCursor = new Position() {
                                                };
    private long                     period     = 1000;                                                 // 单位ms
    private Set<ClientIdentity>      updateCursorTasks;

    public void start() {
        super.start();
        Assert.notNull(zooKeeperMetaManager);
        if (!zooKeeperMetaManager.isStart()) {
            zooKeeperMetaManager.start();
        }

        executor = Executors.newScheduledThreadPool(1);
        destinations = MigrateMap.makeComputingMap(destination -> zooKeeperMetaManager.listAllSubscribeInfo(destination));

        cursors = MigrateMap.makeComputingMap(clientIdentity -> {
            Position position = zooKeeperMetaManager.getCursor(clientIdentity);
            if (position == null) {
                return nullCursor; // 返回一个空对象标识，避免出现异常
            } else {
                return position;
            }
        });

        batches = MigrateMap.makeComputingMap(clientIdentity -> {
            // 读取一下zookeeper信息，初始化一次
            MemoryClientIdentityBatch batches = MemoryClientIdentityBatch.create(clientIdentity);
            Map<Long, PositionRange> positionRanges = zooKeeperMetaManager.listAllBatchs(clientIdentity);
            for (Map.Entry<Long, PositionRange> entry : positionRanges.entrySet()) {
                batches.addPositionRange(entry.getValue(), entry.getKey()); // 添加记录到指定batchId
            }
            return batches;
        });

        updateCursorTasks = Collections.synchronizedSet(new HashSet<>());

        // 启动定时工作任务
        executor.scheduleAtFixedRate(() -> {
            List<ClientIdentity> tasks = new ArrayList<>(updateCursorTasks);
            for (ClientIdentity clientIdentity : tasks) {
                try {
                    updateCursorTasks.remove(clientIdentity);

                    // 定时将内存中的最新值刷到zookeeper中，多次变更只刷一次
                    zooKeeperMetaManager.updateCursor(clientIdentity, getCursor(clientIdentity));
                } catch (Throwable e) {
                    // ignore
                    logger.error("period update" + clientIdentity.toString() + " curosr failed!", e);
                }
            }
        }, period, period, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        super.stop();

        if (zooKeeperMetaManager.isStart()) {
            zooKeeperMetaManager.stop();
        }

        executor.shutdownNow();
        destinations.clear();
        batches.clear();
    }

    public void subscribe(final ClientIdentity clientIdentity) throws CanalMetaManagerException {
        super.subscribe(clientIdentity);

        // 订阅信息频率发生比较低，不需要做定时merge处理
        executor.submit(() -> zooKeeperMetaManager.subscribe(clientIdentity));
    }

    public void unsubscribe(final ClientIdentity clientIdentity) throws CanalMetaManagerException {
        super.unsubscribe(clientIdentity);

        // 订阅信息频率发生比较低，不需要做定时merge处理
        executor.submit(() -> zooKeeperMetaManager.unsubscribe(clientIdentity));
    }

    public void updateCursor(ClientIdentity clientIdentity, Position position) throws CanalMetaManagerException {
        super.updateCursor(clientIdentity, position);
        updateCursorTasks.add(clientIdentity);// 添加到任务队列中进行触发
    }

    public Position getCursor(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        Position position = super.getCursor(clientIdentity);
        if (position == nullCursor) {
            return null;
        } else {
            return position;
        }
    }

    // =============== setter / getter ================

    public void setZooKeeperMetaManager(ZooKeeperMetaManager zooKeeperMetaManager) {
        this.zooKeeperMetaManager = zooKeeperMetaManager;
    }

    public void setPeriod(long period) {
        this.period = period;
    }

}
