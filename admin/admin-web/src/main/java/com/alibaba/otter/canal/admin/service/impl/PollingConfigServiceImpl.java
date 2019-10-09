package com.alibaba.otter.canal.admin.service.impl;

import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.otter.canal.admin.common.exception.ServiceException;
import com.alibaba.otter.canal.admin.model.CanalCluster;
import com.alibaba.otter.canal.admin.model.CanalConfig;
import com.alibaba.otter.canal.admin.model.CanalInstanceConfig;
import com.alibaba.otter.canal.admin.model.NodeServer;
import com.alibaba.otter.canal.admin.service.CanalClusterService;
import com.alibaba.otter.canal.admin.service.NodeServerService;
import com.alibaba.otter.canal.admin.service.PollingConfigService;
import com.alibaba.otter.canal.protocol.SecurityUtil;
import com.google.common.base.Joiner;

@Service
public class PollingConfigServiceImpl implements PollingConfigService {

    @Autowired
    NodeServerService   nodeServerService;

    @Autowired
    CanalClusterService canalClusterService;

    public boolean autoRegister(String ip, Integer adminPort, String cluster) {
        NodeServer server = NodeServer.find.query().where().eq("ip", ip).eq("adminPort", adminPort).findOne();
        if (server == null) {
            server = new NodeServer();
            server.setName(ip);
            server.setIp(ip);
            server.setAdminPort(adminPort);
            server.setTcpPort(adminPort + 1);
            server.setMetricPort(adminPort + 2);
            if (StringUtils.isNotEmpty(cluster)) {
                CanalCluster clusterConfig = CanalCluster.find.query().where().eq("name", cluster).findOne();
                if (clusterConfig == null) {
                    throw new ServiceException("auto cluster : " + cluster + " is not found.");
                }

                server.setClusterId(clusterConfig.getId());
            }
            nodeServerService.save(server);
        }

        return true;
    }

    public CanalConfig getChangedConfig(String ip, Integer port, String md5) {
        NodeServer server = NodeServer.find.query().where().eq("ip", ip).eq("adminPort", port).findOne();
        if (server == null) {
            return null;
        }
        CanalConfig canalConfig;
        if (server.getClusterId() != null) { // 集群模式
            canalConfig = CanalConfig.find.query().where().eq("clusterId", server.getClusterId()).findOne();
        } else { // 单机模式
            canalConfig = CanalConfig.find.query().where().eq("serverId", server.getId()).findOne();
        }
        if (canalConfig == null) {
            throw new ServiceException("canal.properties config is empty");
        }

        if (!canalConfig.getContentMd5().equals(md5)) { // 内容发生变化
            return canalConfig;
        }
        return null;
    }

    public CanalInstanceConfig getInstancesConfig(String ip, Integer port, String md5) {
        NodeServer server = NodeServer.find.query().where().eq("ip", ip).eq("adminPort", port).findOne();
        if (server == null) {
            return null;
        }
        List<CanalInstanceConfig> canalInstanceConfigs;
        if (server.getClusterId() != null) { // 集群模式
            canalInstanceConfigs = CanalInstanceConfig.find.query()
                .where()
                .eq("status", "1")
                .eq("clusterId", server.getClusterId())
                .findList(); // 取属于该集群的所有instance config
        } else { // 单机模式
            canalInstanceConfigs = CanalInstanceConfig.find.query()
                .where()
                .eq("status", "1")
                .eq("serverId", server.getId())
                .findList();
        }

        CanalInstanceConfig canalInstanceConfig = new CanalInstanceConfig();
        List<String> instances = canalInstanceConfigs.stream()
            .map(CanalInstanceConfig::getName)
            .collect(Collectors.toList());
        String data = Joiner.on(',').join(instances);
        canalInstanceConfig.setContent(data);
        if (!StringUtils.isEmpty(md5)) {
            try {
                String newMd5 = SecurityUtil.md5String(canalInstanceConfig.getContent());
                if (StringUtils.equals(md5, newMd5)) {
                    canalInstanceConfig.setContent(null);
                }
            } catch (NoSuchAlgorithmException e) {
                // ignore
            }
        }
        return canalInstanceConfig;
    }

    public CanalInstanceConfig getInstanceConfig(String destination, String md5) {
        CanalInstanceConfig instanceConfig = CanalInstanceConfig.find.query().where().eq("name", destination).findOne();
        if (instanceConfig == null) {
            return null;
        }
        if (StringUtils.isEmpty(md5)) {
            return instanceConfig;
        } else {
            try {
                String newMd5 = SecurityUtil.md5String(instanceConfig.getContent());
                if (StringUtils.equals(md5, newMd5)) {
                    instanceConfig.setContent(null);
                }
            } catch (NoSuchAlgorithmException e) {
                // ignore
            }

            return instanceConfig;
        }
    }
}
