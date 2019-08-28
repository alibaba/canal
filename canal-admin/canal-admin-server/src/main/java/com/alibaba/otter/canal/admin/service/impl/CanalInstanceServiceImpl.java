package com.alibaba.otter.canal.admin.service.impl;

import com.alibaba.otter.canal.admin.common.DaemonThreadFactory;
import com.alibaba.otter.canal.admin.common.exception.ServiceException;
import com.alibaba.otter.canal.protocol.SecurityUtil;
import io.ebean.Query;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Service;

import com.alibaba.otter.canal.admin.connector.AdminConnector;
import com.alibaba.otter.canal.admin.connector.SimpleAdminConnectors;
import com.alibaba.otter.canal.admin.model.CanalInstanceConfig;
import com.alibaba.otter.canal.admin.model.NodeServer;
import com.alibaba.otter.canal.admin.service.CanalInstanceService;

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
            .select("name, modifiedTime")
            .fetch("canalCluster", "name")
            .fetch("nodeServer", "name");
        if (canalInstanceConfig != null) {
            if (StringUtils.isNotEmpty(canalInstanceConfig.getName())) {
                query.where().like("name", "%" + canalInstanceConfig.getName() + "%");
            }
        }
        query.order().asc("id");
        List<CanalInstanceConfig> canalInstanceConfigs = query.findList();

        // check all canal instances running status
        List<NodeServer> nodeServers = NodeServer.find.query().findList();

        if (nodeServers.isEmpty()) {
            return canalInstanceConfigs;
        }

        // ExecutorService executorService =
        // Executors.newFixedThreadPool(nodeServers.size(),
        // DaemonThreadFactory.daemonThreadFactory);
        // List<Future<Void>> futures = new ArrayList<>(nodeServers.size());
        //
        // for (NodeServer nodeServer : nodeServers) {
        // futures.add(executorService.submit(() -> {
        // String runningInstances = SimpleAdminConnectors
        // .execute(nodeServer.getIp(), nodeServer.getAdminPort(),
        // AdminConnector::getRunningInstances);
        // if (runningInstances == null) {
        // return null;
        // }
        // String[] instances = runningInstances.split(",");
        // for (String instance : instances) {
        // for (CanalInstanceConfig cig : canalInstanceConfigs) {
        // if (instance.equals(cig.getName())) {
        // cig.setNodeId(nodeServer.getId());
        // cig.setNodeIp(nodeServer.getIp());
        // break;
        // }
        // }
        // }
        // return null;
        // }));
        // }
        //
        // futures.forEach(f -> {
        // try {
        // f.get(3, TimeUnit.SECONDS);
        // } catch (TimeoutException | InterruptedException | ExecutionException e) {
        // // ignore
        // }
        // });
        //
        // executorService.shutdownNow();

        return canalInstanceConfigs;
    }

    public void save(CanalInstanceConfig canalInstanceConfig) {
        if (StringUtils.isEmpty(canalInstanceConfig.getClusterServerId())) {
            throw new ServiceException("empty cluster or server id");
        }
        if (canalInstanceConfig.getClusterServerId().startsWith("cluster:")) {
            Long clusterId = Long.parseLong(canalInstanceConfig.getClusterServerId().substring(8));
            canalInstanceConfig.setClusterId(clusterId);
        } else if (canalInstanceConfig.getClusterServerId().startsWith("server:")) {
            Long serverId = Long.parseLong(canalInstanceConfig.getClusterServerId().substring(7));
            canalInstanceConfig.setServerId(serverId);
        }

        try {
            String contentMd5 = SecurityUtil.md5String(canalInstanceConfig.getContent());
            canalInstanceConfig.setContentMd5(contentMd5);
        } catch (NoSuchAlgorithmException e) {
            // ignore
        }

        canalInstanceConfig.insert();
    }

    public CanalInstanceConfig detail(Long id) {
        CanalInstanceConfig canalInstanceConfig = CanalInstanceConfig.find.byId(id);
        if (canalInstanceConfig != null) {
            if (canalInstanceConfig.getClusterId() != null) {
                canalInstanceConfig.setClusterServerId("cluster:" + canalInstanceConfig.getClusterId());
            } else if (canalInstanceConfig.getServerId() != null) {
                canalInstanceConfig.setClusterServerId("server:" + canalInstanceConfig.getServerId());
            }
        }
        return canalInstanceConfig;
    }

    public void updateContent(CanalInstanceConfig canalInstanceConfig) {
        if (StringUtils.isEmpty(canalInstanceConfig.getClusterServerId())) {
            throw new ServiceException("empty cluster or server id");
        }
        if (canalInstanceConfig.getClusterServerId().startsWith("cluster:")) {
            Long clusterId = Long.parseLong(canalInstanceConfig.getClusterServerId().substring(8));
            canalInstanceConfig.setClusterId(clusterId);
            canalInstanceConfig.setServerId(null);
        } else if (canalInstanceConfig.getClusterServerId().startsWith("server:")) {
            Long serverId = Long.parseLong(canalInstanceConfig.getClusterServerId().substring(7));
            canalInstanceConfig.setServerId(serverId);
            canalInstanceConfig.setClusterId(null);
        }

        try {
            String contentMd5 = SecurityUtil.md5String(canalInstanceConfig.getContent());
            canalInstanceConfig.setContentMd5(contentMd5);
        } catch (NoSuchAlgorithmException e) {
            // ignore
        }

        canalInstanceConfig.update("content", "contentMd5", "clusterId", "serverId");
    }

    public void delete(Long id) {
        CanalInstanceConfig canalInstanceConfig = CanalInstanceConfig.find.byId(id);
        if (canalInstanceConfig != null) {
            canalInstanceConfig.delete();
        }
    }

    @Override
    public CanalInstanceConfig findOne(String name) {
        CanalInstanceConfig config = CanalInstanceConfig.find.query()
            .setDisableLazyLoading(true)
            .where()
            .eq("name", name)
            .findOne();
        return config;
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

        String log = SimpleAdminConnectors.execute(nodeServer.getIp(),
            nodeServer.getAdminPort(),
            adminConnector -> adminConnector.instanceLog(canalInstanceConfig.getName(), null, 100));

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
            resutl = SimpleAdminConnectors.execute(nodeServer.getIp(),
                nodeServer.getAdminPort(),
                adminConnector -> adminConnector.startInstance(canalInstanceConfig.getName()));
        } else if ("stop".equals(option)) {
            resutl = SimpleAdminConnectors.execute(nodeServer.getIp(),
                nodeServer.getAdminPort(),
                adminConnector -> adminConnector.stopInstance(canalInstanceConfig.getName()));
        } else {
            return false;
        }

        if (resutl == null) {
            resutl = false;
        }
        return resutl;
    }

}
