package com.alibaba.otter.canal.common.alarm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;

/**
 * 基于log的alarm机制实现
 * 
 * @author jianghang 2012-8-22 下午10:12:35
 * @version 1.0.0
 */
public class LogAlarmHandler extends AbstractCanalLifeCycle implements CanalAlarmHandler {

    private static final Logger logger = LoggerFactory.getLogger(LogAlarmHandler.class);

    public void sendAlarm(String destination, String msg) {
        logger.error("destination:{}[{}]", new Object[] { destination, msg });
    }

}
