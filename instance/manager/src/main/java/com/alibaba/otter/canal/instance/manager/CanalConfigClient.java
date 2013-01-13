package com.alibaba.otter.canal.instance.manager;

import com.alibaba.otter.canal.common.CanalException;
import com.alibaba.otter.shared.common.model.canal.Canal;
import com.alibaba.otter.shared.communication.model.canal.FindCanalEvent;
import com.alibaba.otter.shared.communication.model.canal.FindFilterEvent;

/**
 * 对应canal的配置
 * 
 * @author jianghang 2012-7-4 下午03:09:17
 * @version 1.0.0
 */
public class CanalConfigClient {

    private CanalCommmunicationClient delegate;

    /**
     * 根据对应的destinantion查询Canal信息
     */
    public Canal findCanal(String destination) {
        FindCanalEvent event = new FindCanalEvent();
        event.setDestination(destination);
        try {
            Object obj = delegate.callManager(event);
            if (obj != null && obj instanceof Canal) {
                return (Canal) obj;
            } else {
                throw new CanalException("No Such Canal by [" + destination + "]");
            }
        } catch (Exception e) {
            throw new CanalException("call_manager_error", e);
        }
    }

    /**
     * 根据对应的destinantion查询filter信息
     */
    public String findFilter(String destination) {
        FindFilterEvent event = new FindFilterEvent();
        event.setDestination(destination);
        try {
            Object obj = delegate.callManager(event);
            if (obj != null && obj instanceof String) {
                return (String) obj;
            } else {
                throw new CanalException("No Such Canal by [" + destination + "]");
            }
        } catch (Exception e) {
            throw new CanalException("call_manager_error", e);
        }
    }

    // ================== setter / getter ===============

    public void setDelegate(CanalCommmunicationClient delegate) {
        this.delegate = delegate;
    }

}
