package com.alibaba.otter.canal.common.alarm;

import com.alibaba.otter.canal.common.CanalLifeCycle;

/**
 * canal报警处理机制
 * 
 * @author jianghang 2012-8-22 下午10:08:56
 * @version 1.0.0
 */
public interface CanalAlarmHandler extends CanalLifeCycle {

    /**
     * 发送对应destination的报警
     * 
     * @param destination
     * @param msg
     */
    void sendAlarm(String destination, String msg);
}
