package com.alibaba.otter.canal.filter;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;

public class MutliAviaterFilterTest {

    @Test
    public void test_simple() {
        int count = 5;
        ExecutorService executor = Executors.newFixedThreadPool(count);

        final CountDownLatch countDown = new CountDownLatch(count);
        final AtomicInteger successed = new AtomicInteger(0);
        for (int i = 0; i < count; i++) {
            executor.submit(() -> {
                try {
                    for (int i1 = 0; i1 < 100; i1++) {
                        doRegexTest();
                        // try {
                        // Thread.sleep(10);
                        // } catch (InterruptedException e) {
                        // }
                    }

                    successed.incrementAndGet();
                } finally {
                    countDown.countDown();
                }
            });
        }

        try {
            countDown.await();
        } catch (InterruptedException e) {
        }

        Assert.assertEquals(count, successed.get());
        executor.shutdownNow();
    }

    private void doRegexTest() {
        AviaterRegexFilter filter3 = new AviaterRegexFilter("otter2.otter_stability1|otter1.otter_stability1|"
                                                            + RandomStringUtils.randomAlphabetic(200));
        boolean result = filter3.filter("otter1.otter_stability1");
        Assert.assertEquals(true, result);
        result = filter3.filter("otter2.otter_stability1");
        Assert.assertEquals(true, result);
    }

}
