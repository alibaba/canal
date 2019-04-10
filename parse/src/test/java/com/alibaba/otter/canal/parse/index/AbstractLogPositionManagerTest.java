package com.alibaba.otter.canal.parse.index;

import java.net.InetSocketAddress;
import java.util.Date;

import org.junit.Assert;

import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogIdentity;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import org.junit.Ignore;

@Ignore
public class AbstractLogPositionManagerTest extends AbstractZkTest {

    private static final String MYSQL_ADDRESS = "127.0.0.1";

    public LogPosition doTest(CanalLogPositionManager logPositionManager) {
        LogPosition getPosition = logPositionManager.getLatestIndexBy(destination);
        Assert.assertNull(getPosition);

        LogPosition postion1 = buildPosition(1);
        logPositionManager.persistLogPosition(destination, postion1);
        LogPosition getPosition1 = logPositionManager.getLatestIndexBy(destination);
        Assert.assertEquals(postion1, getPosition1);

        LogPosition postion2 = buildPosition(2);
        logPositionManager.persistLogPosition(destination, postion2);
        LogPosition getPosition2 = logPositionManager.getLatestIndexBy(destination);
        Assert.assertEquals(postion2, getPosition2);
        return postion2;
    }

    protected LogPosition buildPosition(int number) {
        LogPosition position = new LogPosition();
        position.setIdentity(new LogIdentity(new InetSocketAddress(MYSQL_ADDRESS, 3306), 1234L));
        position.setPostion(new EntryPosition("mysql-bin.000000" + number, 106L, new Date().getTime()));
        return position;
    }
}
