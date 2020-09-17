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

import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.protocol.position.LogPosition;

/**
 * Created by yinxiu on 17/3/18. Email: marklin.hz@gmail.com
 */
public class PeriodMixedLogPositionManager extends AbstractLogPositionManager {

    private static final Logger         logger       = LoggerFactory.getLogger(PeriodMixedLogPositionManager.class);

    private MemoryLogPositionManager    memoryLogPositionManager;
    private ZooKeeperLogPositionManager zooKeeperLogPositionManager;
    private ScheduledExecutorService    executorService;

    private long                        period;
    private Set<String>                 persistTasks;

    @SuppressWarnings("serial")
    private final LogPosition           nullPosition = new LogPosition() {
                                                     };

    public PeriodMixedLogPositionManager(MemoryLogPositionManager memoryLogPositionManager,
                                         ZooKeeperLogPositionManager zooKeeperLogPositionManager, long period){
        if (memoryLogPositionManager == null) {
            throw new NullPointerException("null memoryLogPositionManager");
        }

        if (zooKeeperLogPositionManager == null) {
            throw new NullPointerException("null zooKeeperLogPositionManager");
        }

        if (period <= 0) {
            throw new IllegalArgumentException("period must be positive, given: " + period);
        }

        this.memoryLogPositionManager = memoryLogPositionManager;
        this.zooKeeperLogPositionManager = zooKeeperLogPositionManager;
        this.period = period;
        this.persistTasks = Collections.synchronizedSet(new HashSet<>());
        this.executorService = Executors.newScheduledThreadPool(1);
    }

    @Override
    public void stop() {
        super.stop();

        if (zooKeeperLogPositionManager.isStart()) {
            zooKeeperLogPositionManager.stop();
        }

        if (memoryLogPositionManager.isStart()) {
            memoryLogPositionManager.stop();
        }

        executorService.shutdown();
    }

    @Override
    public void start() {
        super.start();

        if (!memoryLogPositionManager.isStart()) {
            memoryLogPositionManager.start();
        }

        if (!zooKeeperLogPositionManager.isStart()) {
            zooKeeperLogPositionManager.start();
        }

        // 启动定时工作任务
        executorService.scheduleAtFixedRate(() -> {
            List<String> tasks = new ArrayList<>(persistTasks);
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
        }, period, period, TimeUnit.MILLISECONDS);
    }

    @Override
    public LogPosition getLatestIndexBy(String destination) {
        LogPosition logPosition = memoryLogPositionManager.getLatestIndexBy(destination);
        if (logPosition == nullPosition) {
            return null;
        } else {
            return logPosition;
        }
    }

    @Override
    public void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException {
        persistTasks.add(destination);
        memoryLogPositionManager.persistLogPosition(destination, logPosition);
    }
}
