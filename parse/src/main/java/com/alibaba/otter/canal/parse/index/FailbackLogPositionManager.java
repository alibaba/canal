package com.alibaba.otter.canal.parse.index;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.protocol.position.LogPosition;

/**
 * Created by yinxiu on 17/3/18. Email: marklin.hz@gmail.com
 * 实现基于failover查找的机制完成meta的操作
 *
 * <pre>
 * 应用场景：比如针对内存buffer，出现HA切换，先尝试从内存buffer区中找到lastest position，如果不存在才尝试找一下meta里消费的信息
 * </pre>
 */
public class FailbackLogPositionManager extends AbstractLogPositionManager {

    private final static Logger           logger = LoggerFactory.getLogger(FailbackLogPositionManager.class);

    private final CanalLogPositionManager primary;
    private final CanalLogPositionManager secondary;

    public FailbackLogPositionManager(CanalLogPositionManager primary, CanalLogPositionManager secondary){
        if (primary == null) {
            throw new NullPointerException("nul primary LogPositionManager");
        }
        if (secondary == null) {
            throw new NullPointerException("nul secondary LogPositionManager");
        }

        this.primary = primary;
        this.secondary = secondary;
    }

    @Override
    public void start() {
        super.start();

        if (!primary.isStart()) {
            primary.start();
        }

        if (!secondary.isStart()) {
            secondary.start();
        }
    }

    @Override
    public void stop() {
        super.stop();

        if (secondary.isStart()) {
            secondary.stop();
        }

        if (primary.isStart()) {
            primary.stop();
        }
    }

    @Override
    public LogPosition getLatestIndexBy(String destination) {
        LogPosition logPosition = primary.getLatestIndexBy(destination);
        if (logPosition != null) {
            return logPosition;
        }
        return secondary.getLatestIndexBy(destination);
    }

    @Override
    public void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException {
        try {
            primary.persistLogPosition(destination, logPosition);
        } catch (CanalParseException e) {
            logger.warn("persistLogPosition use primary log position manager exception. destination: {}, logPosition: {}",
                destination,
                logPosition,
                e);
            secondary.persistLogPosition(destination, logPosition);
        }
    }
}
