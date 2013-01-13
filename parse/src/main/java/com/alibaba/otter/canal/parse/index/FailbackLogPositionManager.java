package com.alibaba.otter.canal.parse.index;

import org.springframework.util.Assert;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.protocol.position.LogPosition;

/**
 * 实现基于failover查找的机制完成meta的操作
 * 
 * <pre>
 * 应用场景：比如针对内存buffer，出现HA切换，先尝试从内存buffer区中找到lastest position，如果不存在才尝试找一下meta里消费的信息
 * </pre>
 * 
 * @author jianghang 2012-7-20 下午02:33:20
 */
public class FailbackLogPositionManager extends AbstractCanalLifeCycle implements CanalLogPositionManager {

    private CanalLogPositionManager primary;
    private CanalLogPositionManager failback;

    public void start() {
        super.start();
        Assert.notNull(primary);
        Assert.notNull(failback);

        if (!primary.isStart()) {
            primary.start();
        }

        if (!failback.isStart()) {
            failback.start();
        }
    }

    public void stop() {
        super.stop();

        if (primary.isStart()) {
            primary.stop();
        }

        if (failback.isStart()) {
            failback.stop();
        }
    }

    public LogPosition getLatestIndexBy(String destination) {
        LogPosition logPosition = primary.getLatestIndexBy(destination);
        if (logPosition == null) {
            return failback.getLatestIndexBy(destination);
        } else {
            return logPosition;
        }
    }

    public void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException {
        try {
            primary.persistLogPosition(destination, logPosition);
        } catch (CanalParseException e) {
            failback.persistLogPosition(destination, logPosition);
        }
    }

    public void setPrimary(CanalLogPositionManager primary) {
        this.primary = primary;
    }

    public void setFailback(CanalLogPositionManager failback) {
        this.failback = failback;
    }

}
