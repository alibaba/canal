package com.alibaba.otter.canal.meta;

import java.util.List;
import java.util.Map;

import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.meta.exception.CanalMetaManagerException;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.protocol.position.PositionRange;

/**
 * meta信息管理器
 * 
 * @author jianghang 2012-6-14 下午09:28:48
 * @author zebin.xuzb
 * @version 1.0.0
 */
public interface CanalMetaManager extends CanalLifeCycle {

    /**
     * 增加一个 client订阅 <br/>
     * 如果 client已经存在，则不做任何修改
     */
    void subscribe(ClientIdentity clientIdentity) throws CanalMetaManagerException;

    /**
     * 判断是否订阅
     */
    boolean hasSubscribe(ClientIdentity clientIdentity) throws CanalMetaManagerException;

    /**
     * 取消client订阅
     */
    void unsubscribe(ClientIdentity clientIdentity) throws CanalMetaManagerException;

    /**
     * 获取 cursor 游标
     */
    Position getCursor(ClientIdentity clientIdentity) throws CanalMetaManagerException;

    /**
     * 更新 cursor 游标
     */
    void updateCursor(ClientIdentity clientIdentity, Position position) throws CanalMetaManagerException;

    /**
     * 根据指定的destination列出当前所有的clientIdentity信息
     */
    List<ClientIdentity> listAllSubscribeInfo(String destination) throws CanalMetaManagerException;

    /**
     * 获得该client最新的一个位置
     */
    PositionRange getFirstBatch(ClientIdentity clientIdentity) throws CanalMetaManagerException;

    /**
     * 获得该clientId最新的一个位置
     */
    PositionRange getLastestBatch(ClientIdentity clientIdentity) throws CanalMetaManagerException;

    /**
     * 为 client 产生一个唯一、递增的id
     */
    Long addBatch(ClientIdentity clientIdentity, PositionRange positionRange) throws CanalMetaManagerException;

    /**
     * 指定batchId，插入batch数据
     */
    void addBatch(ClientIdentity clientIdentity, PositionRange positionRange, Long batchId)
                                                                                           throws CanalMetaManagerException;

    /**
     * 根据唯一messageId，查找对应的数据起始信息
     */
    PositionRange getBatch(ClientIdentity clientIdentity, Long batchId) throws CanalMetaManagerException;

    /**
     * 对一个batch的确认
     */
    PositionRange removeBatch(ClientIdentity clientIdentity, Long batchId) throws CanalMetaManagerException;

    /**
     * 查询当前的所有batch信息
     */
    Map<Long, PositionRange> listAllBatchs(ClientIdentity clientIdentity) throws CanalMetaManagerException;

    /**
     * 清除对应的batch信息
     */
    void clearAllBatchs(ClientIdentity clientIdentity) throws CanalMetaManagerException;

}
