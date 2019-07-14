package com.alibaba.otter.canal.admin.service;

import com.alibaba.otter.canal.admin.model.NodeServer;

import java.util.List;

public interface NodeServerService {

    void save(NodeServer nodeServer);

    NodeServer detail(Long id);

    void update(NodeServer nodeServer);

    void delete(Long id);

    List<NodeServer> findList(NodeServer nodeServer);

    int remoteNodeStatus(String ip, Integer port);

    String remoteCanalLog(Long id);

    boolean remoteOperation(Long id, String option);
}
