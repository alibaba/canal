package com.alibaba.otter.canal.admin.service.impl;

import org.springframework.stereotype.Service;

import com.alibaba.otter.canal.admin.model.CanalConfig;
import com.alibaba.otter.canal.admin.service.CanalConfigService;

/**
 * Canal配置信息业务层
 *
 * @author rewerma 2019-07-13 下午05:12:16
 * @version 1.0.0
 */
@Service
public class CanalConfigServiceImpl implements CanalConfigService {

    public CanalConfig getCanalConfig() {
        return CanalConfig.find.byId(1L);
    }

    public CanalConfig getAdapterConfig() {
        return CanalConfig.find.byId(2L);
    }

    public void updateContent(CanalConfig canalConfig) {
        canalConfig.update("content");
    }
}
