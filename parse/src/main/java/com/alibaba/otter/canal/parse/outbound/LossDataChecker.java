package com.alibaba.otter.canal.parse.outbound;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.erosa.protocol.protobuf.ErosaEntry.Entry;

/**
 * loss数据检验机制
 * 
 * <pre>
 * 校验算法：
 * 1. 根据心跳程序定时产生的心跳数据,比如每30秒产生一条
 * 2. 收集前后两次心跳数据的executeTime，检查对应的时间间隔
 * </pre>
 * 
 * @author jianghang 2012-7-7 下午02:10:07
 * @version 4.1.0
 */
public class LossDataChecker {

    private static final Logger logger          = LoggerFactory.getLogger(LossDataChecker.class);
    private Long                lossInterval    = 5 * 60L;
    private Long                lastExecuteTime = -1L;

    public void process(Entry entry) {
        if (isHeartBeat(entry)) {
            Long executeTime = entry.getHeader().getExecutetime();
            if (lastExecuteTime == -1L) {
                lastExecuteTime = executeTime;
            } else {
                Long minus = executeTime - lastExecuteTime;
                if (minus > lossInterval) {
                    logger.warn("WARN ## may be loss data , last:{},now:{},minus:{}", new Object[] { lastExecuteTime,
                            executeTime, minus });
                }
            }
        }
    }

    private boolean isHeartBeat(Entry entry) {
        return true;
    }

    // ================== setter / getter =================

    public void setLossInterval(Long lossInterval) {
        this.lossInterval = lossInterval;
    }

}
