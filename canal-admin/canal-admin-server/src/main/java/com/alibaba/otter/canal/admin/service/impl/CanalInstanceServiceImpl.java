package com.alibaba.otter.canal.admin.service.impl;

import java.util.List;

import com.alibaba.otter.canal.admin.service.CanalInstanceService;
import org.apache.commons.lang.StringUtils;

import com.alibaba.otter.canal.admin.jmx.CanalServerMXBean;
import com.alibaba.otter.canal.admin.jmx.JMXConnection;
import com.alibaba.otter.canal.admin.model.CanalInstanceConfig;
import com.alibaba.otter.canal.admin.model.NodeServer;

import io.ebean.Query;
import org.springframework.stereotype.Service;

@Service
public class CanalInstanceServiceImpl implements CanalInstanceService {

    public List<CanalInstanceConfig> findList(CanalInstanceConfig canalInstanceConfig) {
        Query<CanalInstanceConfig> query = CanalInstanceConfig.find.query()
            .setDisableLazyLoading(true)
            .select("name, modifiedTime");
        if (canalInstanceConfig != null) {
            if (StringUtils.isNotEmpty(canalInstanceConfig.getName())) {
                query.where().like("name", "%" + canalInstanceConfig.getName() + "%");
            }
        }
        List<CanalInstanceConfig> canalInstanceConfigs = query.findList();

        // check all canal instances running status
        List<NodeServer> nodeServers = NodeServer.find.query().findList();
        for (NodeServer nodeServer : nodeServers) {
            String runningInstances = JMXConnection
                .execute(nodeServer.getIp(), nodeServer.getPort(), CanalServerMXBean::getRunningInstances);
            if (runningInstances == null) {
                continue;
            }
            String[] instances = runningInstances.split(",");
            for (String instance : instances) {
                for (CanalInstanceConfig cig : canalInstanceConfigs) {
                    if (instance.equals(cig.getName())) {
                        cig.setNodeServer(nodeServer);
                        cig.setStatus(1);
                        break;
                    }
                }
            }
        }

        return canalInstanceConfigs;
    }

    public void save(CanalInstanceConfig canalInstanceConfig) {
        canalInstanceConfig.insert();
    }

    public CanalInstanceConfig detail(Long id) {
        return CanalInstanceConfig.find.byId(id);
    }

    public void updateContent(CanalInstanceConfig canalInstanceConfig) {
        canalInstanceConfig.update("content");
    }

    public void delete(Long id) {
        CanalInstanceConfig canalInstanceConfig = CanalInstanceConfig.find.byId(id);
        if (canalInstanceConfig != null) {
            canalInstanceConfig.delete();
        }
    }

}
