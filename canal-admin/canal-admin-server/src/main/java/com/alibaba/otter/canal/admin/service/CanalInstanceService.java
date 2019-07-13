package com.alibaba.otter.canal.admin.service;

import com.alibaba.otter.canal.admin.model.CanalInstanceConfig;

import java.util.List;

public interface CanalInstanceService {

    List<CanalInstanceConfig> findList(CanalInstanceConfig canalInstanceConfig);

    void save(CanalInstanceConfig canalInstanceConfig);

    CanalInstanceConfig detail(Long id);

    void updateContent(CanalInstanceConfig canalInstanceConfig);

    void delete(Long id);

    boolean remoteOperation(Long id, Long nodeId, String option);
}
