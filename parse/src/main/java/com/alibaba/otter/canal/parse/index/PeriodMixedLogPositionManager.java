package com.alibaba.otter.canal.parse.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.google.common.base.Function;
import com.google.common.collect.MigrateMap;

/**
 * 基于定时刷新的策略的mixed实现
 * 
 * @author jianghang 2012-9-12 上午11:18:14
 * @version 1.0.0
 */
public class PeriodMixedLogPositionManager extends MemoryLogPositionManager implements CanalLogPositionManager {

    private static final Logger         logger       = LoggerFactory.getLogger(PeriodMixedLogPositionManager.class);
    private ZooKeeperLogPositionManager zooKeeperLogPositionManager;
    private ScheduledExecutorService    executor;
    @SuppressWarnings("serial")
    private final LogPosition           nullPosition = new LogPosition() {
                                                     };

    private long                        period       = 1000;                                                        // 单位ms
    private Set<String>                 persistTasks;

    public void start() {
        super.start();

        Assert.notNull(zooKeeperLogPositionManager);
        if (!zooKeeperLogPositionManager.isStart()) {
            zooKeeperLogPositionManager.start();
        }
        executor = Executors.newScheduledThreadPool(1);
        positions = MigrateMap.makeComputingMap(new Function<String, LogPosition>() {

            public LogPosition apply(String destination) {
                LogPosition logPosition = zooKeeperLogPositionManager.getLatestIndexBy(destination);
                if (logPosition == null) {
                    return nullPosition;
                } else {
                    return logPosition;
                }
            }
        });

        persistTasks = Collections.synchronizedSet(new HashSet<String>());

        // 启动定时工作任务
        executor.scheduleAtFixedRate(new Runnable() {

            public void run() {
                List<String> tasks = new ArrayList<String>(persistTasks);
                for (String destination : tasks) {
                    try {
                        // 定时将内存中的最新值刷到zookeeper中，多次变更只刷一次
                        zooKeeperLogPositionManager.persistLogPosition(destination, getLatestIndexBy(destination));
                        persistTasks.remove(destination);
                    } catch (Throwable e) {
                        // ignore
                        logger.error("period update" + destination + " curosr failed!", e);
                    }
                }
            }
        }, period, period, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        super.stop();

        if (zooKeeperLogPositionManager.isStart()) {
            zooKeeperLogPositionManager.stop();
        }
        executor.shutdownNow();
        positions.clear();
    }

    public void persistLogPosition(String destination, LogPosition logPosition) {
        persistTasks.add(destination);// 添加到任务队列中进行触发
        super.persistLogPosition(destination, logPosition);
    }

    public LogPosition getLatestIndexBy(String destination) {
        LogPosition logPostion = super.getLatestIndexBy(destination);
        if (logPostion == nullPosition) {
            return null;
        } else {
            return logPostion;
        }
    }

    public void setZooKeeperLogPositionManager(ZooKeeperLogPositionManager zooKeeperLogPositionManager) {
        this.zooKeeperLogPositionManager = zooKeeperLogPositionManager;
    }

    public void setPeriod(long period) {
        this.period = period;
    }

}
