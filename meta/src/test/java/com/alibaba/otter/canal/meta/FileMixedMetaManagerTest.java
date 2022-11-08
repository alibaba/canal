package com.alibaba.otter.canal.meta;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.protocol.position.PositionRange;
@Ignore
public class FileMixedMetaManagerTest extends AbstractMetaManagerTest {

    private static final String tmp     = System.getProperty("java.io.tmpdir", "/tmp");
    private static final File   dataDir = new File(tmp, "canal");

    @Before
    public void setUp() {
        try {
            FileUtils.deleteDirectory(dataDir);
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testSubscribeAll() {
        FileMixedMetaManager metaManager = new FileMixedMetaManager();
        metaManager.setDataDirByFile(dataDir);
        metaManager.setPeriod(100);

        metaManager.start();
        doSubscribeTest(metaManager);

        sleep(2000L);
        // 重新构建一次，能获得上一次zk上的记录
        FileMixedMetaManager metaManager2 = new FileMixedMetaManager();
        metaManager2.setDataDirByFile(dataDir);
        metaManager2.setPeriod(100);
        metaManager2.start();

        List<ClientIdentity> clients = metaManager2.listAllSubscribeInfo(destination);
        Assert.assertEquals(2, clients.size());
        metaManager.stop();
    }

    @Test
    public void testBatchAll() {
        FileMixedMetaManager metaManager = new FileMixedMetaManager();
        metaManager.setDataDirByFile(dataDir);
        metaManager.setPeriod(100);

        metaManager.start();
        doBatchTest(metaManager);

        metaManager.clearAllBatchs(clientIdentity);
        Map<Long, PositionRange> ranges = metaManager.listAllBatchs(clientIdentity);
        Assert.assertEquals(0, ranges.size());
        metaManager.stop();
    }

    @Test
    public void testCursorAll() {
        FileMixedMetaManager metaManager = new FileMixedMetaManager();
        metaManager.setDataDirByFile(dataDir);
        metaManager.setPeriod(100);
        metaManager.start();

        Position lastPosition = doCursorTest(metaManager);

        sleep(1000L);
        // 重新构建一次，能获得上一次zk上的记录
        FileMixedMetaManager metaManager2 = new FileMixedMetaManager();
        metaManager2.setDataDirByFile(dataDir);
        metaManager2.setPeriod(100);
        metaManager2.start();

        Position position = metaManager2.getCursor(clientIdentity);
        Assert.assertEquals(position, lastPosition);
        metaManager.stop();
    }
}
