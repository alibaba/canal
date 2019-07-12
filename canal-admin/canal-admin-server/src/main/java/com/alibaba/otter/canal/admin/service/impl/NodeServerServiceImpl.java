package com.alibaba.otter.canal.admin.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import com.alibaba.otter.canal.admin.common.exception.ServiceException;
import com.alibaba.otter.canal.admin.jmx.CanalServerMXBean;
import com.alibaba.otter.canal.admin.jmx.JMXConnection;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.alibaba.otter.canal.admin.model.NodeServer;
import com.alibaba.otter.canal.admin.service.NodeServerService;

import io.ebean.Query;

@Service
public class NodeServerServiceImpl implements NodeServerService {

    private static final Logger logger = LoggerFactory.getLogger(NodeServerServiceImpl.class);

    public void save(NodeServer nodeServer) {
        int cnt = NodeServer.find.query()
            .where()
            .eq("ip", nodeServer.getIp())
            .eq("port", nodeServer.getPort())
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
        nodeServer.update("name", "ip", "port", "port2");
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
                int status = -1;
                JMXConnection jmxConnection = new JMXConnection(ns.getIp(), ns.getPort());
                try {
                    CanalServerMXBean canalServerMXBean = jmxConnection.getCanalServerMXBean();
                    status = canalServerMXBean.getStatus();
                } catch (Exception e) {
                    logger.warn(e.getMessage());
                } finally {
                    jmxConnection.close();
                }
                ns.setStatus(status);
                return status != -1;
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
        JMXConnection jmxConnection = new JMXConnection(ip, port);
        try {
            CanalServerMXBean canalServerMXBean = jmxConnection.getCanalServerMXBean();
            return canalServerMXBean.getStatus();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            jmxConnection.close();
        }
        return -1;
    }
}
