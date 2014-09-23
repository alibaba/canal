package com.alibaba.otter.canal.instance.manager;

import com.alibaba.otter.canal.instance.manager.model.Canal;

/**
 * 对应canal的配置
 * 
 * @author jianghang 2012-7-4 下午03:09:17
 * @version 1.0.0
 */
public class CanalConfigClient {

    /**
     * 根据对应的destinantion查询Canal信息
     */
    public Canal findCanal(String destination) {
        // TODO 根据自己的业务实现
        throw new UnsupportedOperationException();
    }

    /**
     * 根据对应的destinantion查询filter信息
     */
    public String findFilter(String destination) {
        // TODO 根据自己的业务实现
        throw new UnsupportedOperationException();
    }

}
