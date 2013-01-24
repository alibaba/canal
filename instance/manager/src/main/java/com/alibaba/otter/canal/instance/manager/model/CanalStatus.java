package com.alibaba.otter.canal.instance.manager.model;

/**
 * 运行状态
 * 
 * @author jianghang 2012-7-13 下午12:54:13
 * @version 1.0.0
 */
public enum CanalStatus {
    /** 启动 */
    START,
    /** 停止 */
    STOP;

    public boolean isStart() {
        return this.equals(CanalStatus.START);
    }

    public boolean isStop() {
        return this.equals(CanalStatus.STOP);
    }
}
