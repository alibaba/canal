package com.alibaba.otter.canal.parse.ha;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.parse.CanalHASwitchable;
import com.alibaba.otter.canal.parse.inbound.HeartBeatCallback;

/**
 * 基于HeartBeat信息的HA控制 , 注意：非线程安全，需要做做多例化
 * 
 * @author jianghang 2012-7-6 下午02:33:30
 * @version 1.0.0
 */
public class HeartBeatHAController extends AbstractCanalLifeCycle implements CanalHAController, HeartBeatCallback {

    private static final Logger logger              = LoggerFactory.getLogger(HeartBeatHAController.class);
    // default 3 times
    private int                 detectingRetryTimes = 3;
    private int                 failedTimes         = 0;
    private boolean             switchEnable        = false;
    private CanalHASwitchable   eventParser;

    public HeartBeatHAController(){

    }

    public void onSuccess(long costTime) {
        failedTimes = 0;
    }

    public void onFailed(Throwable e) {
        failedTimes++;
        // 检查一下是否超过失败次数
        synchronized (this) {
            if (failedTimes > detectingRetryTimes) {
                if (switchEnable) {
                    eventParser.doSwitch();// 通知执行一次切换
                    failedTimes = 0;
                } else {
                    logger.warn("HeartBeat failed Times:{} , should auto switch ?", failedTimes);
                }
            }
        }
    }

    // ============================= setter / getter
    // ============================

    public void setCanalHASwitchable(CanalHASwitchable canalHASwitchable) {
        this.eventParser = canalHASwitchable;
    }

    public void setDetectingRetryTimes(int detectingRetryTimes) {
        this.detectingRetryTimes = detectingRetryTimes;
    }

    public void setSwitchEnable(boolean switchEnable) {
        this.switchEnable = switchEnable;
    }

}
