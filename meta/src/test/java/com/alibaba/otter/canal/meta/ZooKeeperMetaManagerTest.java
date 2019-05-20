package com.alibaba.otter.canal.meta;

import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.common.zookeeper.ZookeeperPathUtils;
import com.alibaba.otter.canal.protocol.position.PositionRange;
@Ignore
public class ZooKeeperMetaManagerTest extends AbstractMetaManagerTest {

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
    public void testSubscribeAll() {
        ZooKeeperMetaManager metaManager = new ZooKeeperMetaManager();
        metaManager.setZkClientx(zkclientx);
        metaManager.start();

        doSubscribeTest(metaManager);
        metaManager.stop();
    }

    @Test
    public void testBatchAll() {
        ZooKeeperMetaManager metaManager = new ZooKeeperMetaManager();
        metaManager.setZkClientx(zkclientx);
        metaManager.start();
        doBatchTest(metaManager);

        metaManager.clearAllBatchs(clientIdentity);
        Map<Long, PositionRange> ranges = metaManager.listAllBatchs(clientIdentity);
        Assert.assertEquals(0, ranges.size());
        metaManager.stop();
    }

    @Test
    public void testCursorhAll() {
        ZooKeeperMetaManager metaManager = new ZooKeeperMetaManager();
        metaManager.setZkClientx(zkclientx);
        metaManager.start();
        doCursorTest(metaManager);
        metaManager.stop();
    }
}
