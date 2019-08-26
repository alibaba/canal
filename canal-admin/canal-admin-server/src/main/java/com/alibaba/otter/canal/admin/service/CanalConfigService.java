package com.alibaba.otter.canal.admin.service;

import com.alibaba.otter.canal.admin.model.CanalConfig;

public interface CanalConfigService {

    CanalConfig getCanalConfig();

    CanalConfig getCanalConfigSummary();

    CanalConfig getAdapterConfig();

    void updateContent(CanalConfig canalConfig);
}
