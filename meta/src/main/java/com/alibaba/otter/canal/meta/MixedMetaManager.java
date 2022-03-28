package com.alibaba.otter.canal.meta;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.util.Assert;

import com.alibaba.otter.canal.meta.exception.CanalMetaManagerException;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.protocol.position.PositionRange;
import com.google.common.collect.MigrateMap;

/**
 * 组合memory + zookeeper的使用模式
 *
 * @author jianghang 2012-7-11 下午03:58:00
 * @version 1.0.0
 */

public class MixedMetaManager extends MemoryMetaManager implements CanalMetaManager {

    private ExecutorService      executor;
    private ZooKeeperMetaManager zooKeeperMetaManager;
    @SuppressWarnings("serial")
    private final Position       nullCursor = new Position() {
                                            };

    public void start() {
        super.start();
        Assert.notNull(zooKeeperMetaManager);
        if (!zooKeeperMetaManager.isStart()) {
            zooKeeperMetaManager.start();
        }

        executor = Executors.newFixedThreadPool(1);
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

        executor.submit(() -> zooKeeperMetaManager.subscribe(clientIdentity));
    }

    public void unsubscribe(final ClientIdentity clientIdentity) throws CanalMetaManagerException {
        super.unsubscribe(clientIdentity);

        executor.submit(() -> zooKeeperMetaManager.unsubscribe(clientIdentity));
    }

    public void updateCursor(final ClientIdentity clientIdentity, final Position position)
                                                                                          throws CanalMetaManagerException {
        super.updateCursor(clientIdentity, position);

        // 异步刷新
        executor.submit(() -> zooKeeperMetaManager.updateCursor(clientIdentity, position));
    }

    @Override
    public Position getCursor(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        Position position = super.getCursor(clientIdentity);
        if (position == nullCursor) {
            return null;
        } else {
            return position;
        }
    }

    public Long addBatch(final ClientIdentity clientIdentity, final PositionRange positionRange)
                                                                                                throws CanalMetaManagerException {
        final Long batchId = super.addBatch(clientIdentity, positionRange);
        // 异步刷新
        executor.submit(() -> zooKeeperMetaManager.addBatch(clientIdentity, positionRange, batchId));
        return batchId;
    }

    public void addBatch(final ClientIdentity clientIdentity, final PositionRange positionRange, final Long batchId)
                                                                                                                    throws CanalMetaManagerException {
        super.addBatch(clientIdentity, positionRange, batchId);
        // 异步刷新
        executor.submit(() -> zooKeeperMetaManager.addBatch(clientIdentity, positionRange, batchId));
    }

    public PositionRange removeBatch(final ClientIdentity clientIdentity, final Long batchId)
                                                                                             throws CanalMetaManagerException {
        PositionRange positionRange = super.removeBatch(clientIdentity, batchId);
        // 异步刷新
        executor.submit(() -> {
            zooKeeperMetaManager.removeBatch(clientIdentity, batchId);
        });

        return positionRange;
    }

    public void clearAllBatchs(final ClientIdentity clientIdentity) throws CanalMetaManagerException {
        super.clearAllBatchs(clientIdentity);

        // 异步刷新
        executor.submit(() -> zooKeeperMetaManager.clearAllBatchs(clientIdentity));
    }

    // =============== setter / getter ================
    public void setZooKeeperMetaManager(ZooKeeperMetaManager zooKeeperMetaManager) {
        this.zooKeeperMetaManager = zooKeeperMetaManager;
    }
}
