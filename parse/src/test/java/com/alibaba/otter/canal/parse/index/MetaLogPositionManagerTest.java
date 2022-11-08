package com.alibaba.otter.canal.parse.index;

import java.net.InetSocketAddress;
import java.util.Date;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.common.zookeeper.ZookeeperPathUtils;
import com.alibaba.otter.canal.meta.MixedMetaManager;
import com.alibaba.otter.canal.meta.ZooKeeperMetaManager;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogIdentity;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.protocol.position.PositionRange;
@Ignore
public class MetaLogPositionManagerTest extends AbstractLogPositionManagerTest {

    private static final String MYSQL_ADDRESS = "127.0.0.1";
    private ZkClientx           zkclientx     = new ZkClientx(cluster1 + ";" + cluster2);

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
        MixedMetaManager metaManager = new MixedMetaManager();

        ZooKeeperMetaManager zooKeeperMetaManager = new ZooKeeperMetaManager();
        zooKeeperMetaManager.setZkClientx(zkclientx);

        metaManager.setZooKeeperMetaManager(zooKeeperMetaManager);
        metaManager.start();

        MetaLogPositionManager logPositionManager = new MetaLogPositionManager(metaManager);
        logPositionManager.start();
        // 构建meta信息
        ClientIdentity client1 = new ClientIdentity(destination, (short) 1);
        metaManager.subscribe(client1);

        PositionRange range1 = buildRange(1);
        metaManager.updateCursor(client1, range1.getEnd());

        PositionRange range2 = buildRange(2);
        metaManager.updateCursor(client1, range2.getEnd());

        ClientIdentity client2 = new ClientIdentity(destination, (short) 2);
        metaManager.subscribe(client2);

        PositionRange range3 = buildRange(3);
        metaManager.updateCursor(client2, range3.getEnd());

        PositionRange range4 = buildRange(4);
        metaManager.updateCursor(client2, range4.getEnd());

        LogPosition logPosition = logPositionManager.getLatestIndexBy(destination);
        Assert.assertEquals(range2.getEnd(), logPosition);

        metaManager.stop();
        logPositionManager.stop();
    }

    private PositionRange<LogPosition> buildRange(int number) {
        LogPosition start = new LogPosition();
        start.setIdentity(new LogIdentity(new InetSocketAddress(MYSQL_ADDRESS, 3306), 1234L));
        start.setPostion(new EntryPosition("mysql-bin.000000" + number, 106L, new Date().getTime()));

        LogPosition end = new LogPosition();
        end.setIdentity(new LogIdentity(new InetSocketAddress(MYSQL_ADDRESS, 3306), 1234L));
        end.setPostion(new EntryPosition("mysql-bin.000000" + (number + 1), 106L, (new Date().getTime()) + 1000 * 1000L));
        return new PositionRange<>(start, end);
    }
}
