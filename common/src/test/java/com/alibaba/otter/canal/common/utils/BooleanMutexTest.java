package com.alibaba.otter.canal.common.utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

public class BooleanMutexTest {

    public static final int CONCURRENCY = 10;
    private ExecutorService executorService;

    @Before
    public void setUp() {
        executorService = Executors.newFixedThreadPool(CONCURRENCY);
    }

    @After
    public void tearDown() {
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }

    @Test(timeout = 3000L)
    public void testBooleanMutexGet() throws Exception {

        BooleanMutex mutex = new BooleanMutex();

        AtomicLong atomicLong = new AtomicLong(0);

        Phaser phaser = new Phaser(CONCURRENCY + 1);

        for (int i = 0; i < CONCURRENCY; i++) {
            executorService.submit(() -> {
                try {
                    // arrive phase1 and wait until phase1 finish
                    int phase1 = phaser.arrive();
                    phaser.awaitAdvanceInterruptibly(phase1);

                    mutex.get();
                    atomicLong.addAndGet(1);

                    // arrive phase2
                    phaser.arrive();
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            });
        }

        assertEquals(0, atomicLong.get());

        // arrive phase1 and wait until phase1 finish
        int phase1 = phaser.arrive();
        phaser.awaitAdvanceInterruptibly(phase1);
        assertEquals(0, atomicLong.get());

        mutex.set(true);

        // arrive phase2 and wait until phase2 finish
        int phase2 = phaser.arrive();
        phaser.awaitAdvanceInterruptibly(phase2);
        assertEquals(CONCURRENCY, atomicLong.get());
    }


    @Test(timeout = 30000L, expected = TimeoutException.class)
    public void testBooleanMutexBlock() throws Exception {

        BooleanMutex mutex = new BooleanMutex();

        AtomicLong atomicLong = new AtomicLong(0);

        Phaser phaser = new Phaser(CONCURRENCY + 1);

        for (int i = 0; i < CONCURRENCY; i++) {
            executorService.submit(() -> {
                try {
                    // arrive phase1 and wait until phase1 finish
                    int phase1 = phaser.arrive();
                    phaser.awaitAdvanceInterruptibly(phase1);

                    mutex.get();
                    atomicLong.addAndGet(1);

                    // arrive phase2
                    phaser.arrive();
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            });
        }

        assertEquals(0, atomicLong.get());

        // arrive phase1 and wait until phase1 finish
        int phase1 = phaser.arrive();
        phaser.awaitAdvanceInterruptibly(phase1);
        assertEquals(0, atomicLong.get());

        // mutex is still false
        mutex.set(false);


        // arrive phase2 and wait until phase2 finish
        int phase2 = phaser.arrive();
        // will throw interrupted exception when timeout because mutex is still false
        phaser.awaitAdvanceInterruptibly(phase2, 2, TimeUnit.SECONDS);

        fail("unreachable code");
    }
}