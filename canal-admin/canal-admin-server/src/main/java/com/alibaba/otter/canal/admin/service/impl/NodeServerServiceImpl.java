package com.alibaba.otter.canal.admin.service.impl;

import io.ebean.Query;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Service;

import com.alibaba.otter.canal.admin.common.exception.ServiceException;
import com.alibaba.otter.canal.admin.connector.AdminConnector;
import com.alibaba.otter.canal.admin.connector.SimpleAdminConnectors;
import com.alibaba.otter.canal.admin.model.NodeServer;
import com.alibaba.otter.canal.admin.service.NodeServerService;

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
            .eq("admin_port", nodeServer.getAdminPort())
            .findCount();
        if (cnt > 0) {
            throw new ServiceException("节点信息已存在");
        }

        nodeServer.save();
    }

    public NodeServer detail(Long id) {
        return NodeServer.find.byId(id);
    }

    public void update(NodeServer nodeServer) {
        int cnt = NodeServer.find.query()
            .where()
            .eq("ip", nodeServer.getIp())
            .eq("admin_port", nodeServer.getAdminPort())
            .ne("id", nodeServer.getId())
            .findCount();
        if (cnt > 0) {
            throw new ServiceException("节点信息已存在");
        }

        nodeServer.update("name", "ip", "admin_port", "tcp_port", "metric_port");
    }

    public void delete(Long id) {
        NodeServer nodeServer = NodeServer.find.byId(id);
        if (nodeServer != null) {
            nodeServer.delete();
        }
    }

    public List<NodeServer> findList(NodeServer nodeServer) {
        Query<NodeServer> query = NodeServer.find.query();
        if (nodeServer != null) {
            if (StringUtils.isNotEmpty(nodeServer.getName())) {
                query.where().like("name", "%" + nodeServer.getName() + "%");
            }
            if (StringUtils.isNotEmpty(nodeServer.getIp())) {
                query.where().eq("ip", nodeServer.getIp());
            }
        }
        query.order().asc("id");
        List<NodeServer> nodeServers = query.findList();
        if (nodeServers.isEmpty()) {
            return nodeServers;
        }

        ExecutorService executorService = Executors.newFixedThreadPool(nodeServers.size());
        List<Future<Boolean>> futures = new ArrayList<>(nodeServers.size());
        // get all nodes status
        for (NodeServer ns : nodeServers) {
            futures.add(executorService.submit(() -> {
                boolean status = SimpleAdminConnectors.execute(ns.getIp(), ns.getAdminPort(), AdminConnector::check);
                ns.setStatus(status ? "1" : "0");
                return !status;
            }));
        }
        futures.forEach(f -> {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                // ignore
            }
        });

        executorService.shutdownNow();

        return nodeServers;
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
