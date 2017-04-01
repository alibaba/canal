package com.alibaba.otter.canal.parse.index;

import org.I0Itec.zkclient.exception.ZkNoNodeException;

import com.alibaba.otter.canal.common.utils.JsonUtils;
import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.common.zookeeper.ZookeeperPathUtils;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.protocol.position.LogPosition;

/**
 * Created by yinxiu on 17/3/17. Email: marklin.hz@gmail.com
 */
public class ZooKeeperLogPositionManager extends AbstractLogPositionManager {

    private final ZkClientx zkClientx;

    public ZooKeeperLogPositionManager(ZkClientx zkClient){
        if (zkClient == null) {
            throw new NullPointerException("null zkClient");
        }

        this.zkClientx = zkClient;
    }

    @Override
    public LogPosition getLatestIndexBy(String destination) {
        String path = ZookeeperPathUtils.getParsePath(destination);
        byte[] data = zkClientx.readData(path, true);
        if (data == null || data.length == 0) {
            return null;
        }

        return JsonUtils.unmarshalFromByte(data, LogPosition.class);
    }

    @Override
    public void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException {
        String path = ZookeeperPathUtils.getParsePath(destination);
        byte[] data = JsonUtils.marshalToByte(logPosition);
        try {
            zkClientx.writeData(path, data);
        } catch (ZkNoNodeException e) {
            zkClientx.createPersistent(path, data, true);
        }
    }

}
