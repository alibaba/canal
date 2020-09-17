package com.alibaba.otter.canal.meta;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.meta.exception.CanalMetaManagerException;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.protocol.position.PositionRange;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.MigrateMap;

/**
 * 内存版实现
 * 
 * @author zebin.xuzb @ 2012-7-2
 * @version 1.0.0
 */
public class MemoryMetaManager extends AbstractCanalLifeCycle implements CanalMetaManager {

    protected Map<String, List<ClientIdentity>>              destinations;
    protected Map<ClientIdentity, MemoryClientIdentityBatch> batches;
    protected Map<ClientIdentity, Position>                  cursors;

    public void start() {
        super.start();

        batches = MigrateMap.makeComputingMap(MemoryClientIdentityBatch::create);

        cursors = new MapMaker().makeMap();

        destinations = MigrateMap.makeComputingMap(destination -> Lists.newArrayList());
    }

    public void stop() {
        super.stop();

        destinations.clear();
        cursors.clear();
        for (MemoryClientIdentityBatch batch : batches.values()) {
            batch.clearPositionRanges();
        }
    }

    public synchronized void subscribe(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        List<ClientIdentity> clientIdentitys = destinations.get(clientIdentity.getDestination());

        if (clientIdentitys.contains(clientIdentity)) {
            clientIdentitys.remove(clientIdentity);
        }

        clientIdentitys.add(clientIdentity);
    }

    public synchronized boolean hasSubscribe(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        List<ClientIdentity> clientIdentitys = destinations.get(clientIdentity.getDestination());
        return clientIdentitys != null && clientIdentitys.contains(clientIdentity);
    }

    public synchronized void unsubscribe(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        List<ClientIdentity> clientIdentitys = destinations.get(clientIdentity.getDestination());
        if (clientIdentitys != null && clientIdentitys.contains(clientIdentity)) {
            clientIdentitys.remove(clientIdentity);
        }
    }

    public synchronized List<ClientIdentity> listAllSubscribeInfo(String destination) throws CanalMetaManagerException {
        // fixed issue #657, fixed ConcurrentModificationException
        return Lists.newArrayList(destinations.get(destination));
    }

    public Position getCursor(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        return cursors.get(clientIdentity);
    }

    public void updateCursor(ClientIdentity clientIdentity, Position position) throws CanalMetaManagerException {
        cursors.put(clientIdentity, position);
    }

    public Long addBatch(ClientIdentity clientIdentity, PositionRange positionRange) throws CanalMetaManagerException {
        return batches.get(clientIdentity).addPositionRange(positionRange);
    }

    public void addBatch(ClientIdentity clientIdentity, PositionRange positionRange, Long batchId)
                                                                                                  throws CanalMetaManagerException {
        batches.get(clientIdentity).addPositionRange(positionRange, batchId);// 添加记录到指定batchId
    }

    public PositionRange removeBatch(ClientIdentity clientIdentity, Long batchId) throws CanalMetaManagerException {
        return batches.get(clientIdentity).removePositionRange(batchId);
    }

    public PositionRange getBatch(ClientIdentity clientIdentity, Long batchId) throws CanalMetaManagerException {
        return batches.get(clientIdentity).getPositionRange(batchId);
    }

    public PositionRange getLastestBatch(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        return batches.get(clientIdentity).getLastestPositionRange();
    }

    public PositionRange getFirstBatch(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        return batches.get(clientIdentity).getFirstPositionRange();
    }

    public Map<Long, PositionRange> listAllBatchs(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        return batches.get(clientIdentity).listAllPositionRange();
    }

    public void clearAllBatchs(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        batches.get(clientIdentity).clearPositionRanges();
    }

    // ============================

    public static class MemoryClientIdentityBatch {

        private ClientIdentity           clientIdentity;
        private Map<Long, PositionRange> batches          = new MapMaker().makeMap();
        private AtomicLong               atomicMaxBatchId = new AtomicLong(1);

        public static MemoryClientIdentityBatch create(ClientIdentity clientIdentity) {
            return new MemoryClientIdentityBatch(clientIdentity);
        }

        public MemoryClientIdentityBatch(){

        }

        protected MemoryClientIdentityBatch(ClientIdentity clientIdentity){
            this.clientIdentity = clientIdentity;
        }

        public synchronized void addPositionRange(PositionRange positionRange, Long batchId) {
            updateMaxId(batchId);
            batches.put(batchId, positionRange);
        }

        public synchronized Long addPositionRange(PositionRange positionRange) {
            Long batchId = atomicMaxBatchId.getAndIncrement();
            batches.put(batchId, positionRange);
            return batchId;
        }

        public synchronized PositionRange removePositionRange(Long batchId) {
            if (batches.containsKey(batchId)) {
                Long minBatchId = Collections.min(batches.keySet());
                if (!minBatchId.equals(batchId)) {
                    // 检查一下提交的ack/rollback，必须按batchId分出去的顺序提交，否则容易出现丢数据
                    throw new CanalMetaManagerException(String.format("batchId:%d is not the firstly:%d",
                        batchId,
                        minBatchId));
                }
                return batches.remove(batchId);
            } else {
                return null;
            }
        }

        public synchronized PositionRange getPositionRange(Long batchId) {
            return batches.get(batchId);
        }

        public synchronized PositionRange getLastestPositionRange() {
            if (batches.size() == 0) {
                return null;
            } else {
                Long batchId = Collections.max(batches.keySet());
                return batches.get(batchId);
            }
        }

        public synchronized PositionRange getFirstPositionRange() {
            if (batches.size() == 0) {
                return null;
            } else {
                Long batchId = Collections.min(batches.keySet());
                return batches.get(batchId);
            }
        }

        public synchronized Map<Long, PositionRange> listAllPositionRange() {
            Set<Long> batchIdSets = batches.keySet();
            List<Long> batchIds = Lists.newArrayList(batchIdSets);
            Collections.sort(Lists.newArrayList(batchIds));

            return Maps.newHashMap(batches);
        }

        public synchronized void clearPositionRanges() {
            batches.clear();
        }

        private synchronized void updateMaxId(Long batchId) {
            if (atomicMaxBatchId.get() < batchId + 1) {
                atomicMaxBatchId.set(batchId + 1);
            }
        }

        // ============ setter & getter =========

        public ClientIdentity getClientIdentity() {
            return clientIdentity;
        }

        public void setClientIdentity(ClientIdentity clientIdentity) {
            this.clientIdentity = clientIdentity;
        }

    }

}
