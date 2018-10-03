package com.alibaba.otter.canal.client.rocketmq;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ConsumerBatchMessage<T> {

    private final BlockingQueue<T> data;
    private CountDownLatch         latch;
    private boolean                hasFailure = false;

    public ConsumerBatchMessage(BlockingQueue<T> data){
        this.data = data;
        latch = new CountDownLatch(data.size());
    }

    public boolean waitFinish(long timeout) throws InterruptedException {
        return latch.await(timeout, TimeUnit.MILLISECONDS);
    }

    public boolean isSuccess() {
        return !hasFailure;
    }

    public BlockingQueue<T> getData() {
        return data;
    }

    /**
     * Countdown if the sub message is successful.
     */
    public void ack() {
        latch.countDown();
    }

    /**
     * Countdown and fail-fast if the sub message is failed.
     */
    public void fail() {
        hasFailure = true;
        // fail fast
        long count = latch.getCount();
        for (int i = 0; i < count; i++) {
            latch.countDown();
        }
    }
}
