package com.alibaba.otter.canal.parse.index;

import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.protocol.position.LogPosition;

/**
 * Created by yinxiu on 17/3/17. Email: marklin.hz@gmail.com
 */
public interface CanalLogPositionManager extends CanalLifeCycle {

    LogPosition getLatestIndexBy(String destination);

    void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException;

    /**
     * 一般使用在异常场景清理缓存，这样持久化的存储更新后能够得到最新的数据，不需要重启机器
     */
   default void removeLogPositionCache(String destination){}

}
