package com.alibaba.otter.canal.parse.index;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.alibaba.otter.canal.protocol.position.LogPosition;
@Ignore
public class FileMixedLogPositionManagerTest extends AbstractLogPositionManagerTest {

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
    public void testAll() {
        MemoryLogPositionManager memoryLogPositionManager = new MemoryLogPositionManager();

        FileMixedLogPositionManager logPositionManager = new FileMixedLogPositionManager(dataDir,
            1000,
            memoryLogPositionManager);
        logPositionManager.start();

        LogPosition position2 = doTest(logPositionManager);
        sleep(1500);

        FileMixedLogPositionManager logPositionManager2 = new FileMixedLogPositionManager(dataDir,
            1000,
            memoryLogPositionManager);
        logPositionManager2.start();

        LogPosition getPosition2 = logPositionManager2.getLatestIndexBy(destination);
        Assert.assertEquals(position2, getPosition2);

        logPositionManager.stop();
        logPositionManager2.stop();
    }
}
