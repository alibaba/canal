package com.alibaba.otter.canal.admin.service;

import java.util.List;

import com.alibaba.otter.canal.admin.model.CanalCluster;

public interface CanalClusterService {

    void save(CanalCluster canalCluster);

    CanalCluster detail(Long id);

    void update(CanalCluster canalCluster);

    void delete(Long id);

    List<CanalCluster> findList(CanalCluster canalCluster);
}
