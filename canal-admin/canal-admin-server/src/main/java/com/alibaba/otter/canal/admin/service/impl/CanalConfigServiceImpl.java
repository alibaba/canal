package com.alibaba.otter.canal.admin.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.otter.canal.admin.dao.CanalConfigDao;
import com.alibaba.otter.canal.admin.model.CanalConfig;
import com.alibaba.otter.canal.admin.service.CanalConfigService;

@Service
public class CanalConfigServiceImpl implements CanalConfigService {

    @Autowired
    CanalConfigDao canalConfigDao;

    public CanalConfig getCanalConfig() {
        return canalConfigDao.findById(1L);
    }

    public CanalConfig getAdapterConfig() {
        return canalConfigDao.findById(2L);
    }

    public void updateContent(CanalConfig canalConfig) {
        canalConfigDao.findById(canalConfig.getId());
    }
}
