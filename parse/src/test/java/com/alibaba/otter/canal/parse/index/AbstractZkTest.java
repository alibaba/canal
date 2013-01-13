package com.alibaba.otter.canal.parse.index;

import org.junit.Assert;

public class AbstractZkTest {

    protected String destination = "ljhtest1";
    protected String cluster1    = "10.20.153.52:2188";
    protected String cluster2    = "10.20.153.52:2188,10.20.153.52:2188";

    public void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            Assert.fail(e.getMessage());
        }
    }
}
