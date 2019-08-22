package com.alibaba.otter.canal.store.memory.buffer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.store.CanalStoreException;
import com.alibaba.otter.canal.store.helper.CanalEventUtils;
import com.alibaba.otter.canal.store.memory.MemoryEventStoreWithBuffer;
import com.alibaba.otter.canal.store.model.Event;
import com.alibaba.otter.canal.store.model.Events;

/**
 * 测试普通的put / get操作
 * 
 * @author jianghang 2012-6-19 下午09:50:08
 * @version 1.0.0
 */
public class MemoryEventStorePutAndGetTest extends MemoryEventStoreBase {

    @Test
    public void testOnePut() {
        MemoryEventStoreWithBuffer eventStore = new MemoryEventStoreWithBuffer();
        eventStore.start();
        // 尝试阻塞
        try {
            eventStore.put(buildEvent("1", 1L, 1L));
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
    public void testFullPut() {
        int bufferSize = 16;
        MemoryEventStoreWithBuffer eventStore = new MemoryEventStoreWithBuffer();
        eventStore.setBufferSize(bufferSize);
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

    @Test
    public void testBlockPutOneGet() {
        final MemoryEventStoreWithBuffer eventStore = new MemoryEventStoreWithBuffer();
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
}
