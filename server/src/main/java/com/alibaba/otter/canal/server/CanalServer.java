package com.alibaba.otter.canal.server;

import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.server.exception.CanalServerException;

/**
 * 对应canal整个服务实例，一个jvm实例只有一份server
 * 
 * @author jianghang 2012-7-12 下午01:32:29
 * @version 1.0.0
 */
public interface CanalServer extends CanalLifeCycle {

    void start() throws CanalServerException;

    void stop() throws CanalServerException;
}
