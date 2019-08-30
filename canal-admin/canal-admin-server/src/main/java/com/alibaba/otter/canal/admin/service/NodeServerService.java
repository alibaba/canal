package com.alibaba.otter.canal.admin.service;

import com.alibaba.otter.canal.admin.model.NodeServer;
import com.alibaba.otter.canal.admin.model.Pager;

import java.util.List;

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
