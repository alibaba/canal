package com.alibaba.otter.canal.admin.service;

import java.util.List;

import com.alibaba.otter.canal.admin.model.NodeServer;
import com.alibaba.otter.canal.admin.model.Pager;

public interface NodeServerService {

    void save(NodeServer nodeServer);

    NodeServer detail(Long id);

    void update(NodeServer nodeServer);

    void delete(Long id);

    List<NodeServer> findAll(NodeServer nodeServer);

    Pager<NodeServer> findList(NodeServer nodeServer, Pager<NodeServer> pager);

    int remoteNodeStatus(String ip, Integer port);

    String remoteCanalLog(Long id);

    boolean remoteOperation(Long id, String option);
}
