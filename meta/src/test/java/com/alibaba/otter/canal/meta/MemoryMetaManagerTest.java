package com.alibaba.otter.canal.meta;

import java.util.Map;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.alibaba.otter.canal.protocol.position.PositionRange;
@Ignore
public class MemoryMetaManagerTest extends AbstractMetaManagerTest {

    @Test
    public void testSubscribeAll() {
        MemoryMetaManager metaManager = new MemoryMetaManager();
        metaManager.start();
        doSubscribeTest(metaManager);
        metaManager.stop();
    }

    @Test
    public void testBatchAll() {
        MemoryMetaManager metaManager = new MemoryMetaManager();
        metaManager.start();
        doBatchTest(metaManager);

        metaManager.clearAllBatchs(clientIdentity);
        Map<Long, PositionRange> ranges = metaManager.listAllBatchs(clientIdentity);
        Assert.assertEquals(0, ranges.size());
        metaManager.stop();
    }

    @Test
    public void testCursorAll() {
        MemoryMetaManager metaManager = new MemoryMetaManager();
        metaManager.start();
        doCursorTest(metaManager);
        metaManager.stop();
    }
}
