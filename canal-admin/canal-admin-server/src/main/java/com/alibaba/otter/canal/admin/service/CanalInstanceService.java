package com.alibaba.otter.canal.admin.service;

import com.alibaba.otter.canal.admin.model.CanalInstanceConfig;

import java.util.List;
import java.util.Map;

public interface CanalInstanceService {

    List<CanalInstanceConfig> findList(CanalInstanceConfig canalInstanceConfig);

    void save(CanalInstanceConfig canalInstanceConfig);

    CanalInstanceConfig detail(Long id);

    void updateContent(CanalInstanceConfig canalInstanceConfig);

    void delete(Long id);

    Map<String, String> remoteInstanceLog(Long id, Long nodeId);

    boolean remoteOperation(Long id, Long nodeId, String option);
}
