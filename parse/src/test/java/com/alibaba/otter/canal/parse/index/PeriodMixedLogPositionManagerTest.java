package com.alibaba.otter.canal.parse.index;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.common.zookeeper.ZookeeperPathUtils;
import com.alibaba.otter.canal.protocol.position.LogPosition;

public class PeriodMixedLogPositionManagerTest extends AbstractLogPositionManagerTest {

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

    @Test
    public void testAll() {
        PeriodMixedLogPositionManager logPositionManager = new PeriodMixedLogPositionManager();

        ZooKeeperLogPositionManager zookeeperLogPositionManager = new ZooKeeperLogPositionManager();
        zookeeperLogPositionManager.setZkClientx(zkclientx);

        logPositionManager.setZooKeeperLogPositionManager(zookeeperLogPositionManager);
        logPositionManager.start();

        LogPosition position2 = doTest(logPositionManager);
        sleep(1500);

        PeriodMixedLogPositionManager logPositionManager2 = new PeriodMixedLogPositionManager();
        logPositionManager2.setZooKeeperLogPositionManager(zookeeperLogPositionManager);
        logPositionManager2.start();

        LogPosition getPosition2 = logPositionManager2.getLatestIndexBy(destination);
        Assert.assertEquals(position2, getPosition2);

        logPositionManager.stop();
        logPositionManager2.stop();
    }
}
