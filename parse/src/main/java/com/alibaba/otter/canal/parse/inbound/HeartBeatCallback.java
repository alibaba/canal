package com.alibaba.otter.canal.parse.inbound;

/**
 * 提供mysql heartBeat心跳数据的callback机制
 * 
 * @author jianghang 2012-6-26 下午04:49:56
 * @version 1.0.0
 */
public interface HeartBeatCallback {

    /**
     * 心跳发送成功
     */
    public void onSuccess(long costTime);

    /**
     * 心跳发送失败
     */
    public void onFailed(Throwable e);

}
