package com.alibaba.otter.canal.meta;

import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.common.zookeeper.ZookeeperPathUtils;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.protocol.position.PositionRange;
@Ignore
public class MixedMetaManagerTest extends AbstractMetaManagerTest {

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
        MixedMetaManager metaManager = new MixedMetaManager();

        ZooKeeperMetaManager zooKeeperMetaManager = new ZooKeeperMetaManager();
        zooKeeperMetaManager.setZkClientx(zkclientx);

        metaManager.setZooKeeperMetaManager(zooKeeperMetaManager);
        metaManager.start();
        doSubscribeTest(metaManager);

        sleep(1000L);
        // 重新构建一次，能获得上一次zk上的记录
        MixedMetaManager metaManager2 = new MixedMetaManager();
        metaManager2.setZooKeeperMetaManager(zooKeeperMetaManager);
        metaManager2.start();

        List<ClientIdentity> clients = metaManager2.listAllSubscribeInfo(destination);
        Assert.assertEquals(2, clients.size());
        metaManager.stop();
    }

    @Test
    public void testBatchAll() {
        MixedMetaManager metaManager = new MixedMetaManager();

        ZooKeeperMetaManager zooKeeperMetaManager = new ZooKeeperMetaManager();
        zooKeeperMetaManager.setZkClientx(zkclientx);

        metaManager.setZooKeeperMetaManager(zooKeeperMetaManager);
        metaManager.start();
        doBatchTest(metaManager);

        sleep(1000L);
        // 重新构建一次，能获得上一次zk上的记录
        MixedMetaManager metaManager2 = new MixedMetaManager();
        metaManager2.setZooKeeperMetaManager(zooKeeperMetaManager);
        metaManager2.start();

        Map<Long, PositionRange> ranges = metaManager2.listAllBatchs(clientIdentity);
        Assert.assertEquals(3, ranges.size());

        metaManager.clearAllBatchs(clientIdentity);
        ranges = metaManager.listAllBatchs(clientIdentity);
        Assert.assertEquals(0, ranges.size());
        metaManager.stop();
        metaManager2.stop();
    }

    @Test
    public void testCursorAll() {
        MixedMetaManager metaManager = new MixedMetaManager();

        ZooKeeperMetaManager zooKeeperMetaManager = new ZooKeeperMetaManager();
        zooKeeperMetaManager.setZkClientx(zkclientx);

        metaManager.setZooKeeperMetaManager(zooKeeperMetaManager);
        metaManager.start();
        Position lastPosition = doCursorTest(metaManager);

        sleep(1000L);
        // 重新构建一次，能获得上一次zk上的记录
        MixedMetaManager metaManager2 = new MixedMetaManager();
        metaManager2.setZooKeeperMetaManager(zooKeeperMetaManager);
        metaManager2.start();

        Position position = metaManager2.getCursor(clientIdentity);
        Assert.assertEquals(position, lastPosition);
        metaManager.stop();
    }
}
