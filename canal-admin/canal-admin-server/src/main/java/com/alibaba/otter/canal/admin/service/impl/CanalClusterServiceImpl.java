package com.alibaba.otter.canal.admin.service.impl;

import java.util.List;

import org.springframework.stereotype.Service;

import com.alibaba.otter.canal.admin.model.CanalCluster;
import com.alibaba.otter.canal.admin.service.CanalClusterServic;

import io.ebean.Query;

@Service
public class CanalClusterServiceImpl implements CanalClusterServic {

    public void save(CanalCluster canalCluster) {
        canalCluster.save();
    }

    public CanalCluster detail(Long id) {
        return CanalCluster.find.byId(id);
    }

    public void update(CanalCluster canalCluster) {
        canalCluster.update("name", "zkHosts");
    }

    public void delete(Long id) {
        CanalCluster canalCluster = CanalCluster.find.byId(id);
        if (canalCluster != null) {
            canalCluster.delete();
        }
    }

    public List<CanalCluster> findList(CanalCluster canalCluster) {
        Query<CanalCluster> query = CanalCluster.find.query();
        query.order().asc("id");
        return query.findList();
    }
}
