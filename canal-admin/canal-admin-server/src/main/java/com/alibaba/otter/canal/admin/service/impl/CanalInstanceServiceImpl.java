package com.alibaba.otter.canal.admin.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Service;

import com.alibaba.otter.canal.admin.jmx.CanalServerMXBean;
import com.alibaba.otter.canal.admin.jmx.JMXConnection;
import com.alibaba.otter.canal.admin.model.CanalInstanceConfig;
import com.alibaba.otter.canal.admin.model.NodeServer;
import com.alibaba.otter.canal.admin.service.CanalInstanceService;

import io.ebean.Query;

/**
 * Canal实例配置信息业务层
 *
 * @author rewerma 2019-07-13 下午05:12:16
 * @version 1.0.0
 */
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
        query.order().asc("id");
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
                        cig.setNodeId(nodeServer.getId());
                        cig.setNodeIp(nodeServer.getIp());
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

    public Map<String, String> remoteInstanceLog(Long id, Long nodeId) {
        Map<String, String> result = new HashMap<>();

        NodeServer nodeServer = NodeServer.find.byId(nodeId);
        if (nodeServer == null) {
            return result;
        }
        CanalInstanceConfig canalInstanceConfig = CanalInstanceConfig.find.byId(id);
        if (canalInstanceConfig == null) {
            return result;
        }

        String log = JMXConnection.execute(nodeServer.getIp(),
            nodeServer.getPort(),
            canalServerMXBean -> canalServerMXBean.instanceLog(canalInstanceConfig.getName()));

        result.put("instance", canalInstanceConfig.getName());
        result.put("log", log);
        return result;
    }

    public boolean remoteOperation(Long id, Long nodeId, String option) {
        NodeServer nodeServer = null;
        if ("start".equals(option)) {
            if (nodeId != null) {
                nodeServer = NodeServer.find.byId(nodeId);
            } else {
                nodeServer = NodeServer.find.query().findOne();
            }
        } else {
            if (nodeId == null) {
                return false;
            }
            nodeServer = NodeServer.find.byId(nodeId);
        }
        if (nodeServer == null) {
            return false;
        }
        CanalInstanceConfig canalInstanceConfig = CanalInstanceConfig.find.byId(id);
        if (canalInstanceConfig == null) {
            return false;
        }
        Boolean resutl = null;
        if ("start".equals(option)) {
            resutl = JMXConnection.execute(nodeServer.getIp(),
                nodeServer.getPort(),
                canalServerMXBean -> canalServerMXBean.startInstance(canalInstanceConfig.getName()));
        } else if ("stop".equals(option)) {
            resutl = JMXConnection.execute(nodeServer.getIp(),
                nodeServer.getPort(),
                canalServerMXBean -> canalServerMXBean.stopInstance(canalInstanceConfig.getName()));
        } else {
            return false;
        }

        if (resutl == null) {
            resutl = false;
        }
        return resutl;
    }
}
