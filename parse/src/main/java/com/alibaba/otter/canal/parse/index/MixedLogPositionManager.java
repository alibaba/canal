package com.alibaba.otter.canal.parse.index;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.google.common.base.Function;
import com.google.common.collect.MapMaker;

/**
 * 混合memory + zookeeper的存储模式
 * 
 * @author jianghang 2012-7-7 上午10:33:19
 * @version 1.0.0
 */
public class MixedLogPositionManager extends MemoryLogPositionManager implements CanalLogPositionManager {

    private static final Logger         logger       = LoggerFactory.getLogger(MixedLogPositionManager.class);
    private ZooKeeperLogPositionManager zooKeeperLogPositionManager;
    private ExecutorService             executor;
    @SuppressWarnings("serial")
    private final LogPosition           nullPosition = new LogPosition() {
                                                     };

    public void start() {
        super.start();

        Assert.notNull(zooKeeperLogPositionManager);
        if (!zooKeeperLogPositionManager.isStart()) {
            zooKeeperLogPositionManager.start();
        }
        executor = Executors.newFixedThreadPool(1);
        positions = new MapMaker().makeComputingMap(new Function<String, LogPosition>() {

            public LogPosition apply(String destination) {
                LogPosition logPosition = zooKeeperLogPositionManager.getLatestIndexBy(destination);
                if (logPosition == null) {
                    return nullPosition;
                } else {
                    return logPosition;
                }
            }
        });
    }

    public void stop() {
        super.stop();

        if (zooKeeperLogPositionManager.isStart()) {
            zooKeeperLogPositionManager.stop();
        }
        executor.shutdownNow();
        positions.clear();
    }

    public void persistLogPosition(final String destination, final LogPosition logPosition) {
        super.persistLogPosition(destination, logPosition);
        executor.submit(new Runnable() {

            public void run() {
                try {
                    zooKeeperLogPositionManager.persistLogPosition(destination, logPosition);
                } catch (Exception e) {
                    logger.error("ERROR # persist to zookeepr has an error", e);
                }
            }
        });

    }

    public LogPosition getLatestIndexBy(String destination) {
        LogPosition logPosition = super.getLatestIndexBy(destination);
        if (logPosition == nullPosition) {
            return null;
        } else {
            return logPosition;
        }
    }

    // ======================== setter / getter ======================

    public void setZooKeeperLogPositionManager(ZooKeeperLogPositionManager zooKeeperLogPositionManager) {
        this.zooKeeperLogPositionManager = zooKeeperLogPositionManager;
    }

}
