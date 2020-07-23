package com.alibaba.otter.canal.admin.service.impl;

import io.ebean.Query;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Service;

import com.alibaba.otter.canal.admin.common.TemplateConfigLoader;
import com.alibaba.otter.canal.admin.common.Threads;
import com.alibaba.otter.canal.admin.common.exception.ServiceException;
import com.alibaba.otter.canal.admin.connector.AdminConnector;
import com.alibaba.otter.canal.admin.connector.SimpleAdminConnectors;
import com.alibaba.otter.canal.admin.model.CanalConfig;
import com.alibaba.otter.canal.admin.model.CanalInstanceConfig;
import com.alibaba.otter.canal.admin.model.NodeServer;
import com.alibaba.otter.canal.admin.model.Pager;
import com.alibaba.otter.canal.admin.service.NodeServerService;
import com.alibaba.otter.canal.protocol.SecurityUtil;

/**
 * 节点信息业务层
 *
 * @author rewerma 2019-07-13 下午05:12:16
 * @version 1.0.0
 */
@Service
public class NodeServerServiceImpl implements NodeServerService {

    public void save(NodeServer nodeServer) {
        int cnt = NodeServer.find.query()
            .where()
            .eq("ip", nodeServer.getIp())
            .eq("adminPort", nodeServer.getAdminPort())
            .findCount();
        if (cnt > 0) {
            throw new ServiceException("节点信息已存在");
        }

        nodeServer.save();

        if (nodeServer.getClusterId() == null) { // 单机模式
            CanalConfig canalConfig = new CanalConfig();
            canalConfig.setServerId(nodeServer.getId());
            String configTmp = TemplateConfigLoader.loadCanalConfig();
            canalConfig.setContent(configTmp);
            try {
                String contentMd5 = SecurityUtil.md5String(canalConfig.getContent());
                canalConfig.setContentMd5(contentMd5);
            } catch (NoSuchAlgorithmException e) {
            }
            canalConfig.save();
        }
    }

    public NodeServer detail(Long id) {
        return NodeServer.find.byId(id);
    }

    public void update(NodeServer nodeServer) {
        int cnt = NodeServer.find.query()
            .where()
            .eq("ip", nodeServer.getIp())
            .eq("adminPort", nodeServer.getAdminPort())
            .ne("id", nodeServer.getId())
            .findCount();
        if (cnt > 0) {
            throw new ServiceException("节点信息已存在");
        }

        nodeServer.update("name", "ip", "adminPort", "tcpPort", "metricPort", "clusterId");
    }

    public void delete(Long id) {
        NodeServer nodeServer = NodeServer.find.byId(id);
        if (nodeServer != null) {
            // 判断是否存在实例
            int cnt = CanalInstanceConfig.find.query().where().eq("serverId", nodeServer.getId()).findCount();
            if (cnt > 0) {
                throw new ServiceException("当前Server下存在Instance配置, 无法删除");
            }

            // 同时删除配置
            CanalConfig canalConfig = CanalConfig.find.query().where().eq("serverId", id).findOne();
            if (canalConfig != null) {
                canalConfig.delete();
            }

            nodeServer.delete();
        }
    }

    private Query<NodeServer> getBaseQuery(NodeServer nodeServer) {
        Query<NodeServer> query = NodeServer.find.query();
        query.fetch("canalCluster", "name").setDisableLazyLoading(true);

        if (nodeServer != null) {
            if (StringUtils.isNotEmpty(nodeServer.getName())) {
                query.where().like("name", "%" + nodeServer.getName() + "%");
            }
            if (StringUtils.isNotEmpty(nodeServer.getIp())) {
                query.where().eq("ip", nodeServer.getIp());
            }
            if (nodeServer.getClusterId() != null) {
                if (nodeServer.getClusterId() == -1) {
                    query.where().isNull("clusterId");
                } else {
                    query.where().eq("clusterId", nodeServer.getClusterId());
                }
            }
        }

        return query;
    }

    public List<NodeServer> findAll(NodeServer nodeServer) {
        Query<NodeServer> query = getBaseQuery(nodeServer);
        query.order().asc("id");
        return query.findList();
    }

    public Pager<NodeServer> findList(NodeServer nodeServer, Pager<NodeServer> pager) {

        Query<NodeServer> query = getBaseQuery(nodeServer);
        Query<NodeServer> queryCnt = query.copy();

        int count = queryCnt.findCount();
        pager.setCount((long) count);

        List<NodeServer> nodeServers = query.order()
            .asc("id")
            .setFirstRow(pager.getOffset().intValue())
            .setMaxRows(pager.getSize())
            .findList();
        pager.setItems(nodeServers);

        if (nodeServers.isEmpty()) {
            return pager;
        }

        List<Future<Boolean>> futures = new ArrayList<>(nodeServers.size());
        // get all nodes status
        for (NodeServer ns : nodeServers) {
            futures.add(Threads.executorService.submit(() -> {
                boolean status = SimpleAdminConnectors.execute(ns.getIp(), ns.getAdminPort(), AdminConnector::check);
                ns.setStatus(status ? "1" : "0");
                return !status;
            }));
        }
        for (Future<Boolean> f : futures) {
            try {
                f.get(3, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException e) {
                // ignore
            } catch (TimeoutException e) {
                break;
            }
        }

        return pager;
    }

    public int remoteNodeStatus(String ip, Integer port) {
        boolean result = SimpleAdminConnectors.execute(ip, port, AdminConnector::check);
        return result ? 1 : 0;
    }

    public String remoteCanalLog(Long id) {
        NodeServer nodeServer = NodeServer.find.byId(id);
        if (nodeServer == null) {
            return "";
        }
        return SimpleAdminConnectors.execute(nodeServer.getIp(),
            nodeServer.getAdminPort(),
            adminConnector -> adminConnector.canalLog(100));
    }

    public boolean remoteOperation(Long id, String option) {
        NodeServer nodeServer = NodeServer.find.byId(id);
        if (nodeServer == null) {
            return false;
        }
        Boolean result = null;
        if ("start".equals(option)) {
            result = SimpleAdminConnectors.execute(nodeServer.getIp(), nodeServer.getAdminPort(), AdminConnector::start);
        } else if ("stop".equals(option)) {
            result = SimpleAdminConnectors.execute(nodeServer.getIp(), nodeServer.getAdminPort(), AdminConnector::stop);
        } else {
            return false;
        }

        if (result == null) {
            result = false;
        }
        return result;
    }
}
