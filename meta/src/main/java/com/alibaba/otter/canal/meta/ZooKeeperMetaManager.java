package com.alibaba.otter.canal.meta;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.commons.lang.StringUtils;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.utils.JsonUtils;
import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.common.zookeeper.ZookeeperPathUtils;
import com.alibaba.otter.canal.meta.exception.CanalMetaManagerException;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.protocol.position.PositionRange;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * zk 版本的 canal manager， 存储结构：
 *
 * <pre>
 * /otter
 *    canal
 *      destinations
 *        dest1
 *          client1
 *            filter
 *            batch_mark
 *              1
 *              2
 *              3
 * </pre>
 *
 * @author zebin.xuzb @ 2012-6-21
 * @author jianghang
 * @version 1.0.0
 */
public class ZooKeeperMetaManager extends AbstractCanalLifeCycle implements CanalMetaManager {

    private static final String ENCODE = "UTF-8";
    private ZkClientx           zkClientx;

    public void start() {
        super.start();

        Assert.notNull(zkClientx);
    }

    public void stop() {
        zkClientx = null; //关闭时置空
        super.stop();
    }

    public void subscribe(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        String path = ZookeeperPathUtils.getClientIdNodePath(clientIdentity.getDestination(),
            clientIdentity.getClientId());

        try {
            zkClientx.createPersistent(path, true);
        } catch (ZkNodeExistsException e) {
            // ignore
        }
        if (clientIdentity.hasFilter()) {
            String filterPath = ZookeeperPathUtils.getFilterPath(clientIdentity.getDestination(),
                clientIdentity.getClientId());

            byte[] bytes = null;
            try {
                bytes = clientIdentity.getFilter().getBytes(ENCODE);
            } catch (UnsupportedEncodingException e) {
                throw new CanalMetaManagerException(e);
            }

            try {
                zkClientx.createPersistent(filterPath, bytes);
            } catch (ZkNodeExistsException e) {
                // ignore
                zkClientx.writeData(filterPath, bytes);
            }
        }
    }

    public boolean hasSubscribe(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        String path = ZookeeperPathUtils.getClientIdNodePath(clientIdentity.getDestination(),
            clientIdentity.getClientId());
        return zkClientx.exists(path);
    }

    public void unsubscribe(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        String path = ZookeeperPathUtils.getClientIdNodePath(clientIdentity.getDestination(),
            clientIdentity.getClientId());
        zkClientx.deleteRecursive(path); // 递归删除所有信息
    }

    public List<ClientIdentity> listAllSubscribeInfo(String destination) throws CanalMetaManagerException {
        if (zkClientx == null) { //重新加载时可能为空
            return new ArrayList<>();
        }
        String path = ZookeeperPathUtils.getDestinationPath(destination);
        List<String> childs = null;
        try {
            childs = zkClientx.getChildren(path);
        } catch (ZkNoNodeException e) {
            // ignore
        }

        if (CollectionUtils.isEmpty(childs)) {
            return new ArrayList<>();
        }
        List<Short> clientIds = new ArrayList<>();
        for (String child : childs) {
            if (StringUtils.isNumeric(child)) {
                clientIds.add(ZookeeperPathUtils.getClientId(child));
            }
        }

        Collections.sort(clientIds); // 进行一个排序
        List<ClientIdentity> clientIdentities = Lists.newArrayList();
        for (Short clientId : clientIds) {
            path = ZookeeperPathUtils.getFilterPath(destination, clientId);
            byte[] bytes = zkClientx.readData(path, true);
            String filter = null;
            if (bytes != null) {
                try {
                    filter = new String(bytes, ENCODE);
                } catch (UnsupportedEncodingException e) {
                    throw new CanalMetaManagerException(e);
                }
            }
            clientIdentities.add(new ClientIdentity(destination, clientId, filter));
        }

        return clientIdentities;
    }

    public Position getCursor(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        String path = ZookeeperPathUtils.getCursorPath(clientIdentity.getDestination(), clientIdentity.getClientId());

        byte[] data = zkClientx.readData(path, true);
        if (data == null || data.length == 0) {
            return null;
        }

        return JsonUtils.unmarshalFromByte(data, Position.class);
    }

    public void updateCursor(ClientIdentity clientIdentity, Position position) throws CanalMetaManagerException {
        String path = ZookeeperPathUtils.getCursorPath(clientIdentity.getDestination(), clientIdentity.getClientId());
        byte[] data = JsonUtils.marshalToByte(position, SerializerFeature.WriteClassName);
        try {
            zkClientx.writeData(path, data);
        } catch (ZkNoNodeException e) {
            zkClientx.createPersistent(path, data, true);// 第一次节点不存在，则尝试重建
        }
    }

    public Long addBatch(ClientIdentity clientIdentity, PositionRange positionRange) throws CanalMetaManagerException {
        String path = ZookeeperPathUtils.getBatchMarkPath(clientIdentity.getDestination(),
            clientIdentity.getClientId());
        byte[] data = JsonUtils.marshalToByte(positionRange, SerializerFeature.WriteClassName);
        String batchPath = zkClientx
            .createPersistentSequential(path + ZookeeperPathUtils.ZOOKEEPER_SEPARATOR, data, true);
        String batchIdString = StringUtils.substringAfterLast(batchPath, ZookeeperPathUtils.ZOOKEEPER_SEPARATOR);
        return ZookeeperPathUtils.getBatchMarkId(batchIdString);
    }

    public void addBatch(ClientIdentity clientIdentity, PositionRange positionRange,
                         Long batchId) throws CanalMetaManagerException {
        String path = ZookeeperPathUtils
            .getBatchMarkWithIdPath(clientIdentity.getDestination(), clientIdentity.getClientId(), batchId);
        byte[] data = JsonUtils.marshalToByte(positionRange, SerializerFeature.WriteClassName);
        zkClientx.createPersistent(path, data, true);
    }

    public PositionRange removeBatch(ClientIdentity clientIdentity, Long batchId) throws CanalMetaManagerException {
        String batchsPath = ZookeeperPathUtils.getBatchMarkPath(clientIdentity.getDestination(),
            clientIdentity.getClientId());
        List<String> nodes = zkClientx.getChildren(batchsPath);
        if (CollectionUtils.isEmpty(nodes)) {
            // 没有batch记录
            return null;
        }

        // 找到最小的Id
        ArrayList<Long> batchIds = new ArrayList<>(nodes.size());
        for (String batchIdString : nodes) {
            batchIds.add(Long.valueOf(batchIdString));
        }
        Long minBatchId = Collections.min(batchIds);
        if (!minBatchId.equals(batchId)) {
            // 检查一下提交的ack/rollback，必须按batchId分出去的顺序提交，否则容易出现丢数据
            throw new CanalMetaManagerException(String.format("batchId:%d is not the firstly:%d", batchId, minBatchId));
        }

        if (!batchIds.contains(batchId)) {
            // 不存在对应的batchId
            return null;
        }
        PositionRange positionRange = getBatch(clientIdentity, batchId);
        if (positionRange != null) {
            String path = ZookeeperPathUtils
                .getBatchMarkWithIdPath(clientIdentity.getDestination(), clientIdentity.getClientId(), batchId);
            zkClientx.delete(path);
        }

        return positionRange;
    }

    public PositionRange getBatch(ClientIdentity clientIdentity, Long batchId) throws CanalMetaManagerException {
        String path = ZookeeperPathUtils
            .getBatchMarkWithIdPath(clientIdentity.getDestination(), clientIdentity.getClientId(), batchId);
        byte[] data = zkClientx.readData(path, true);
        if (data == null) {
            return null;
        }

        PositionRange positionRange = JsonUtils.unmarshalFromByte(data, PositionRange.class);
        return positionRange;
    }

    public void clearAllBatchs(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        String path = ZookeeperPathUtils.getBatchMarkPath(clientIdentity.getDestination(),
            clientIdentity.getClientId());
        List<String> batchChilds = zkClientx.getChildren(path);

        for (String batchChild : batchChilds) {
            String batchPath = path + ZookeeperPathUtils.ZOOKEEPER_SEPARATOR + batchChild;
            zkClientx.delete(batchPath);
        }
    }

    public PositionRange getLastestBatch(ClientIdentity clientIdentity) {
        String path = ZookeeperPathUtils.getBatchMarkPath(clientIdentity.getDestination(),
            clientIdentity.getClientId());
        List<String> nodes = null;
        try {
            nodes = zkClientx.getChildren(path);
        } catch (ZkNoNodeException e) {
            // ignore
        }

        if (CollectionUtils.isEmpty(nodes)) {
            return null;
        }
        // 找到最大的Id
        ArrayList<Long> batchIds = new ArrayList<>(nodes.size());
        for (String batchIdString : nodes) {
            batchIds.add(Long.valueOf(batchIdString));
        }
        Long maxBatchId = Collections.max(batchIds);
        PositionRange result = getBatch(clientIdentity, maxBatchId);
        if (result == null) { // 出现为null，说明zk节点有变化，重新获取
            return getLastestBatch(clientIdentity);
        } else {
            return result;
        }
    }

    public PositionRange getFirstBatch(ClientIdentity clientIdentity) {
        String path = ZookeeperPathUtils.getBatchMarkPath(clientIdentity.getDestination(),
            clientIdentity.getClientId());
        List<String> nodes = null;
        try {
            nodes = zkClientx.getChildren(path);
        } catch (ZkNoNodeException e) {
            // ignore
        }

        if (CollectionUtils.isEmpty(nodes)) {
            return null;
        }
        // 找到最小的Id
        ArrayList<Long> batchIds = new ArrayList<>(nodes.size());
        for (String batchIdString : nodes) {
            batchIds.add(Long.valueOf(batchIdString));
        }
        Long minBatchId = Collections.min(batchIds);
        PositionRange result = getBatch(clientIdentity, minBatchId);
        if (result == null) { // 出现为null，说明zk节点有变化，重新获取
            return getFirstBatch(clientIdentity);
        } else {
            return result;
        }
    }

    public Map<Long, PositionRange> listAllBatchs(ClientIdentity clientIdentity) {
        String path = ZookeeperPathUtils.getBatchMarkPath(clientIdentity.getDestination(),
            clientIdentity.getClientId());
        List<String> nodes = null;
        try {
            nodes = zkClientx.getChildren(path);
        } catch (ZkNoNodeException e) {
            // ignore
        }

        if (CollectionUtils.isEmpty(nodes)) {
            return Maps.newHashMap();
        }
        // 找到最大的Id
        ArrayList<Long> batchIds = new ArrayList<>(nodes.size());
        for (String batchIdString : nodes) {
            batchIds.add(Long.valueOf(batchIdString));
        }

        Collections.sort(batchIds); // 从小到大排序
        Map<Long, PositionRange> positionRanges = Maps.newLinkedHashMap();
        for (Long batchId : batchIds) {
            PositionRange result = getBatch(clientIdentity, batchId);
            if (result == null) {// 出现为null，说明zk节点有变化，重新获取
                return listAllBatchs(clientIdentity);
            } else {
                positionRanges.put(batchId, result);
            }
        }

        return positionRanges;
    }

    // =========== setter ==========

    public void setZkClientx(ZkClientx zkClientx) {
        this.zkClientx = zkClientx;
    }

}
