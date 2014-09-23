package com.alibaba.otter.canal.instance.manager;

import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.instance.core.CanalInstanceGenerator;
import com.alibaba.otter.canal.instance.manager.model.Canal;

/**
 * 基于manager生成对应的{@linkplain CanalInstance}
 * 
 * @author jianghang 2012-7-12 下午05:37:09
 * @version 1.0.0
 */
public class ManagerCanalInstanceGenerator implements CanalInstanceGenerator {

    private CanalConfigClient canalConfigClient;

    public CanalInstance generate(String destination) {
        Canal canal = canalConfigClient.findCanal(destination);
        String filter = canalConfigClient.findFilter(destination);
        return new CanalInstanceWithManager(canal, filter);
    }

    // ================ setter / getter ================

    public void setCanalConfigClient(CanalConfigClient canalConfigClient) {
        this.canalConfigClient = canalConfigClient;
    }

}
