package com.alibaba.otter.canal.meta;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.junit.Assert;

import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogIdentity;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.protocol.position.PositionRange;
import org.junit.Ignore;
import org.junit.Test;
@Ignore
public class AbstractMetaManagerTest extends AbstractZkTest {

    private static final String MYSQL_ADDRESS  = "127.0.0.1";
    protected ClientIdentity    clientIdentity = new ClientIdentity(destination, (short) 1);

    @Test
    public void doSubscribeTest(CanalMetaManager metaManager) {
        ClientIdentity client1 = new ClientIdentity(destination, (short) 1);
        metaManager.subscribe(client1);
        metaManager.subscribe(client1); // 重复调用
        ClientIdentity client2 = new ClientIdentity(destination, (short) 2);
        metaManager.subscribe(client2);

        List<ClientIdentity> clients = metaManager.listAllSubscribeInfo(destination);
        Assert.assertEquals(Arrays.asList(client1, client2), clients);

        metaManager.unsubscribe(client2);
        ClientIdentity client3 = new ClientIdentity(destination, (short) 3);
        metaManager.subscribe(client3);

        clients = metaManager.listAllSubscribeInfo(destination);
        Assert.assertEquals(Arrays.asList(client1, client3), clients);

    }

    @Test
    public void doBatchTest(CanalMetaManager metaManager) {
        metaManager.subscribe(clientIdentity);

        PositionRange first = metaManager.getFirstBatch(clientIdentity);
        PositionRange lastest = metaManager.getLastestBatch(clientIdentity);

        Assert.assertNull(first);
        Assert.assertNull(lastest);

        PositionRange range1 = buildRange(1);
        Long batchId1 = metaManager.addBatch(clientIdentity, range1);

        PositionRange range2 = buildRange(2);
        Long batchId2 = metaManager.addBatch(clientIdentity, range2);
        Assert.assertEquals((batchId1.longValue() + 1), batchId2.longValue());

        // 验证get
        PositionRange getRange1 = metaManager.getBatch(clientIdentity, batchId1);
        Assert.assertEquals(range1, getRange1);
        PositionRange getRange2 = metaManager.getBatch(clientIdentity, batchId2);
        Assert.assertEquals(range2, getRange2);

        PositionRange range3 = buildRange(3);
        Long batchId3 = batchId2 + 1;
        metaManager.addBatch(clientIdentity, range3, batchId3);

        PositionRange range4 = buildRange(4);
        Long batchId4 = metaManager.addBatch(clientIdentity, range4);
        Assert.assertEquals((batchId3.longValue() + 1), batchId4.longValue());

        // 验证remove
        metaManager.removeBatch(clientIdentity, batchId1);
        range1 = metaManager.getBatch(clientIdentity, batchId1);
        Assert.assertNull(range1);

        // 验证first / lastest
        first = metaManager.getFirstBatch(clientIdentity);
        lastest = metaManager.getLastestBatch(clientIdentity);

        Assert.assertEquals(range2, first);
        Assert.assertEquals(range4, lastest);

        Map<Long, PositionRange> ranges = metaManager.listAllBatchs(clientIdentity);
        Assert.assertEquals(3, ranges.size());
    }

    public Position doCursorTest(CanalMetaManager metaManager) {
        metaManager.subscribe(clientIdentity);

        Position position1 = metaManager.getCursor(clientIdentity);
        Assert.assertNull(position1);

        PositionRange range = buildRange(1);

        metaManager.updateCursor(clientIdentity, range.getStart());
        Position position2 = metaManager.getCursor(clientIdentity);
        Assert.assertEquals(range.getStart(), position2);

        metaManager.updateCursor(clientIdentity, range.getEnd());
        Position position3 = metaManager.getCursor(clientIdentity);
        Assert.assertEquals(range.getEnd(), position3);

        return position3;
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
