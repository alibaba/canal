package com.alibaba.otter.canal.parse.index;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.common.zookeeper.ZookeeperPathUtils;

public class ZooKeeperLogPositionManagerTest extends AbstractLogPositionManagerTest {

    private ZkClientx zkclientx = new ZkClientx(cluster1 + ";" + cluster2);

    @Before
    public void setUp() {
        String path = ZookeeperPathUtils.getDestinationPath(destination);
        zkclientx.deleteRecursive(path);
    }
    @After
    public void tearDown() {
        String path = ZookeeperPathUtils.getDestinationPath(destination);
        zkclientx.deleteRecursive(path);
    }

    @Ignore
    @Test
    public void testAll() {
        ZooKeeperLogPositionManager logPositionManager = new ZooKeeperLogPositionManager(zkclientx);
        logPositionManager.start();

        doTest(logPositionManager);
        logPositionManager.stop();
    }
}
