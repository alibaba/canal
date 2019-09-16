package com.alibaba.otter.canal.client;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ConsumerBatchMessage<T> {

    private final List<T>  data;
    private CountDownLatch         latch;
    private boolean                hasFailure = false;

    public ConsumerBatchMessage(List<T> data){
        this.data = data;
        latch = new CountDownLatch(1);
    }

    public boolean waitFinish(long timeout) throws InterruptedException {
        return latch.await(timeout, TimeUnit.MILLISECONDS);
    }

    public boolean isSuccess() {
        return !hasFailure;
    }

    public List<T> getData() {
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
