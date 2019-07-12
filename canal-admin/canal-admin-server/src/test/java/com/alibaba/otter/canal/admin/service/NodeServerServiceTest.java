package com.alibaba.otter.canal.admin.service;

import com.alibaba.otter.canal.admin.BaseTest;
import com.alibaba.otter.canal.admin.model.NodeServer;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

public class NodeServerServiceTest extends BaseTest {

    @Autowired
    NodeServerService nodeServerService;

    @Test
    public void findList() {
        List<NodeServer> list = nodeServerService.findList(null);
        Assert.assertNotNull(list);
    }
}
