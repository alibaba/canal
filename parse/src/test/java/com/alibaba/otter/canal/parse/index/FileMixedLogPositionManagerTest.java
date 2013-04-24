package com.alibaba.otter.canal.parse.index;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.otter.canal.protocol.position.LogPosition;

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
        FileMixedLogPositionManager logPositionManager = new FileMixedLogPositionManager();
        logPositionManager.setDataDir(dataDir);
        logPositionManager.setPeriod(100);
        logPositionManager.start();

        LogPosition position2 = doTest(logPositionManager);
        sleep(1500);

        FileMixedLogPositionManager logPositionManager2 = new FileMixedLogPositionManager();
        logPositionManager2.setDataDir(dataDir);
        logPositionManager2.setPeriod(100);
        logPositionManager2.start();

        LogPosition getPosition2 = logPositionManager2.getLatestIndexBy(destination);
        Assert.assertEquals(position2, getPosition2);

        logPositionManager.stop();
        logPositionManager2.stop();
    }
}
