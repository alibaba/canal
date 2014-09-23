package com.alibaba.otter.canal.client.running;

import org.junit.Assert;

public class AbstractZkTest {

    protected String destination = "ljhtest1";
    protected String cluster1    = "127.0.0.1:2188";
    protected String cluster2    = "127.0.0.1:2188,127.0.0.1:2188";

    public void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            Assert.fail(e.getMessage());
        }
    }
}
