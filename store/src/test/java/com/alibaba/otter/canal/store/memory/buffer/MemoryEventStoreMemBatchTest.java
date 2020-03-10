package com.alibaba.otter.canal.store.memory.buffer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.store.CanalStoreException;
import com.alibaba.otter.canal.store.helper.CanalEventUtils;
import com.alibaba.otter.canal.store.memory.MemoryEventStoreWithBuffer;
import com.alibaba.otter.canal.store.model.BatchMode;
import com.alibaba.otter.canal.store.model.Event;
import com.alibaba.otter.canal.store.model.Events;

public class MemoryEventStoreMemBatchTest extends MemoryEventStoreBase {

    @Test
    public void testOnePut() {
        MemoryEventStoreWithBuffer eventStore = new MemoryEventStoreWithBuffer();
        eventStore.setBatchMode(BatchMode.MEMSIZE);
        eventStore.start();
        // 尝试阻塞
        try {
            eventStore.put(buildEvent("1", 1L, 1L, 1024));
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        // 尝试阻塞+超时
        boolean result = false;
        try {
            result = eventStore.put(buildEvent("1", 1L, 1L), 1000L, TimeUnit.MILLISECONDS);
            Assert.assertTrue(result);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        // 尝试
        result = eventStore.tryPut(buildEvent("1", 1L, 1L));
        Assert.assertTrue(result);

        eventStore.stop();
    }

    @Test
    public void testOnePutExceedLimit() {
        MemoryEventStoreWithBuffer eventStore = new MemoryEventStoreWithBuffer();
        eventStore.setBufferSize(1);
        eventStore.setBatchMode(BatchMode.MEMSIZE);
        eventStore.start();
        // 尝试阻塞
        try {
            boolean result = eventStore.tryPut(buildEvent("1", 1L, 1L, 1025));// 只有一条记录，第一条超过也允许放入
            Assert.assertTrue(result);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        eventStore.stop();
    }

    @Test
    public void testFullPut() {
        int bufferSize = 16;
        MemoryEventStoreWithBuffer eventStore = new MemoryEventStoreWithBuffer();
        eventStore.setBufferSize(bufferSize);
        eventStore.setBatchMode(BatchMode.MEMSIZE);
        eventStore.start();

        for (int i = 0; i < bufferSize; i++) {
            boolean result = eventStore.tryPut(buildEvent("1", 1L, 1L + i));
            Assert.assertTrue(result);
        }

        boolean result = eventStore.tryPut(buildEvent("1", 1L, 1L + bufferSize));
        Assert.assertFalse(result);

        try {
            result = eventStore.put(buildEvent("1", 1L, 1L + bufferSize), 1000L, TimeUnit.MILLISECONDS);
        } catch (CanalStoreException e) {
            Assert.fail(e.getMessage());
        } catch (InterruptedException e) {
            Assert.fail(e.getMessage());
        }

        Assert.assertFalse(result);

        eventStore.stop();
    }

    @Test
    public void testOnePutOneGet() {
        MemoryEventStoreWithBuffer eventStore = new MemoryEventStoreWithBuffer();
        eventStore.setBatchMode(BatchMode.MEMSIZE);
        eventStore.start();

        boolean result = eventStore.tryPut(buildEvent("1", 1L, 1L));
        Assert.assertTrue(result);

        Position position = eventStore.getFirstPosition();
        Events<Event> entrys = eventStore.tryGet(position, 1);
        Assert.assertTrue(entrys.getEvents().size() == 1);
        Assert.assertEquals(position, entrys.getPositionRange().getStart());
        Assert.assertEquals(position, entrys.getPositionRange().getEnd());

        eventStore.stop();
    }

    @Test
    public void testFullPutBatchGet() {
        int bufferSize = 16;
        MemoryEventStoreWithBuffer eventStore = new MemoryEventStoreWithBuffer();
        eventStore.setBufferSize(bufferSize);
        eventStore.setBatchMode(BatchMode.MEMSIZE);
        eventStore.start();

        for (int i = 0; i < bufferSize; i++) {
            boolean result = eventStore.tryPut(buildEvent("1", 1L, 1L + i));
            sleep(100L);
            Assert.assertTrue(result);
        }

        Position first = eventStore.getFirstPosition();
        Position lastest = eventStore.getLatestPosition();
        Assert.assertEquals(first, CanalEventUtils.createPosition(buildEvent("1", 1L, 1L)));
        Assert.assertEquals(lastest, CanalEventUtils.createPosition(buildEvent("1", 1L, 1L + bufferSize - 1)));

        System.out.println("start get");
        Events<Event> entrys1 = eventStore.tryGet(first, bufferSize);
        System.out.println("first get size : " + entrys1.getEvents().size());

        Assert.assertTrue(entrys1.getEvents().size() == bufferSize);
        Assert.assertEquals(first, entrys1.getPositionRange().getStart());
        Assert.assertEquals(lastest, entrys1.getPositionRange().getEnd());

        Assert.assertEquals(first, CanalEventUtils.createPosition(entrys1.getEvents().get(0)));
        Assert.assertEquals(lastest, CanalEventUtils.createPosition(entrys1.getEvents().get(bufferSize - 1)));
        eventStore.stop();
    }

    @Ignore
    @Test
    public void testBlockPutOneGet() {
        final MemoryEventStoreWithBuffer eventStore = new MemoryEventStoreWithBuffer();
        eventStore.setBufferSize(16);
        eventStore.setBatchMode(BatchMode.MEMSIZE);
        eventStore.start();

        final int batchSize = 10;
        for (int i = 0; i < batchSize; i++) {
            boolean result = eventStore.tryPut(buildEvent("1", 1L, 1L));
            Assert.assertTrue(result);
        }

        final Position position = eventStore.getFirstPosition();
        try {
            Events<Event> entrys = eventStore.get(position, batchSize);
            Assert.assertTrue(entrys.getEvents().size() == batchSize);
            Assert.assertEquals(position, entrys.getPositionRange().getStart());
            Assert.assertEquals(position, entrys.getPositionRange().getEnd());
        } catch (CanalStoreException e) {
        } catch (InterruptedException e) {
        }

        ExecutorService executor = Executors.newFixedThreadPool(1);
        executor.submit(new Runnable() {

            public void run() {
                boolean result = false;
                try {
                    eventStore.get(position, batchSize);
                } catch (CanalStoreException e) {
                } catch (InterruptedException e) {
                    System.out.println("interrupt occured.");
                    result = true;
                }
                Assert.assertTrue(result);
            }
        });

        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            Assert.fail(e.getMessage());
        }
        executor.shutdownNow();

        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            Assert.fail(e.getMessage());
        }
        eventStore.stop();
    }

    @Test
    public void testRollback() {
        int bufferSize = 16;
        MemoryEventStoreWithBuffer eventStore = new MemoryEventStoreWithBuffer();
        eventStore.setBufferSize(bufferSize);
        eventStore.setBatchMode(BatchMode.MEMSIZE);
        eventStore.start();

        for (int i = 0; i < bufferSize / 2; i++) {
            boolean result = eventStore.tryPut(buildEvent("1", 1L, 1L + i));
            sleep(100L);
            Assert.assertTrue(result);
        }

        sleep(50L);
        Position first = eventStore.getFirstPosition();
        Position lastest = eventStore.getLatestPosition();
        Assert.assertEquals(first, CanalEventUtils.createPosition(buildEvent("1", 1L, 1L)));
        Assert.assertEquals(lastest, CanalEventUtils.createPosition(buildEvent("1", 1L, 1L + bufferSize / 2 - 1)));

        System.out.println("start get");
        Events<Event> entrys1 = eventStore.tryGet(first, bufferSize);
        System.out.println("first get size : " + entrys1.getEvents().size());

        eventStore.rollback();

        entrys1 = eventStore.tryGet(first, bufferSize);
        System.out.println("after rollback get size : " + entrys1.getEvents().size());
        Assert.assertTrue(entrys1.getEvents().size() == bufferSize / 2);

        // 继续造数据
        for (int i = bufferSize / 2; i < bufferSize; i++) {
            boolean result = eventStore.tryPut(buildEvent("1", 1L, 1L + i));
            sleep(100L);
            Assert.assertTrue(result);
        }

        Events<Event> entrys2 = eventStore.tryGet(entrys1.getPositionRange().getEnd(), bufferSize);
        System.out.println("second get size : " + entrys2.getEvents().size());

        eventStore.rollback();

        entrys2 = eventStore.tryGet(entrys1.getPositionRange().getEnd(), bufferSize);
        System.out.println("after rollback get size : " + entrys2.getEvents().size());
        Assert.assertTrue(entrys2.getEvents().size() == bufferSize);

        first = eventStore.getFirstPosition();
        lastest = eventStore.getLatestPosition();
        List<Event> entrys = new ArrayList<Event>(entrys2.getEvents());
        Assert.assertTrue(entrys.size() == bufferSize);
        Assert.assertEquals(first, entrys2.getPositionRange().getStart());
        Assert.assertEquals(lastest, entrys2.getPositionRange().getEnd());

        Assert.assertEquals(first, CanalEventUtils.createPosition(entrys.get(0)));
        Assert.assertEquals(lastest, CanalEventUtils.createPosition(entrys.get(bufferSize - 1)));
        eventStore.stop();
    }

    @Test
    public void testAck() {
        int bufferSize = 16;
        MemoryEventStoreWithBuffer eventStore = new MemoryEventStoreWithBuffer();
        eventStore.setBufferSize(bufferSize);
        eventStore.setBatchMode(BatchMode.MEMSIZE);
        eventStore.start();

        for (int i = 0; i < bufferSize / 2; i++) {
            boolean result = eventStore.tryPut(buildEvent("1", 1L, 1L + i));
            sleep(100L);
            Assert.assertTrue(result);
        }

        sleep(50L);
        Position first = eventStore.getFirstPosition();
        Position lastest = eventStore.getLatestPosition();
        Assert.assertEquals(first, CanalEventUtils.createPosition(buildEvent("1", 1L, 1L)));
        Assert.assertEquals(lastest, CanalEventUtils.createPosition(buildEvent("1", 1L, 1L + bufferSize / 2 - 1)));

        System.out.println("start get");
        Events<Event> entrys1 = eventStore.tryGet(first, bufferSize);
        System.out.println("first get size : " + entrys1.getEvents().size());

        eventStore.cleanUntil(entrys1.getPositionRange().getEnd());
        sleep(50L);

        // 继续造数据
        for (int i = bufferSize / 2; i < bufferSize; i++) {
            boolean result = eventStore.tryPut(buildEvent("1", 1L, 1L + i));
            sleep(100L);
            Assert.assertTrue(result);
        }

        Events<Event> entrys2 = eventStore.tryGet(entrys1.getPositionRange().getEnd(), bufferSize);
        System.out.println("second get size : " + entrys2.getEvents().size());

        eventStore.rollback();

        entrys2 = eventStore.tryGet(entrys1.getPositionRange().getEnd(), bufferSize);
        System.out.println("after rollback get size : " + entrys2.getEvents().size());

        first = eventStore.getFirstPosition();
        lastest = eventStore.getLatestPosition();
        List<Event> entrys = new ArrayList<Event>(entrys2.getEvents());
        // Assert.assertEquals(first, entrys2.getPositionRange().getStart());
        Assert.assertEquals(lastest, entrys2.getPositionRange().getEnd());

        // Assert.assertEquals(first,
        // CanalEventUtils.createPosition(entrys.get(0)));
        Assert.assertEquals(lastest, CanalEventUtils.createPosition(entrys.get(entrys.size() - 1)));

        // 全部ack掉
        eventStore.cleanUntil(entrys2.getPositionRange().getEnd());

        // 最后就拿不到数据
        Events<Event> entrys3 = eventStore.tryGet(entrys1.getPositionRange().getEnd(), bufferSize);
        System.out.println("third get size : " + entrys3.getEvents().size());
        Assert.assertEquals(0, entrys3.getEvents().size());

        eventStore.stop();
    }
}
