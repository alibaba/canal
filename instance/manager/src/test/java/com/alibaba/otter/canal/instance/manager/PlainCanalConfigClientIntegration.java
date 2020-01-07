package com.alibaba.otter.canal.instance.manager;

import org.junit.Test;
import org.springframework.util.Assert;

import com.alibaba.otter.canal.instance.manager.plain.PlainCanal;
import com.alibaba.otter.canal.instance.manager.plain.PlainCanalConfigClient;

public class PlainCanalConfigClientIntegration {

    @Test
    public void testSimple() {
        PlainCanalConfigClient client = new PlainCanalConfigClient("http://127.0.0.1:8089",
            "admin",
            "4ACFE3202A5FF5CF467898FC58AAB1D615029441",
            "127.0.0.1",
            11110);

        PlainCanal plain = client.findServer(null);
        Assert.notNull(plain);

        plain = client.findServer(plain.getMd5());
        Assert.isNull(plain);

        String instances = client.findInstances(null);
        Assert.notNull(instances);

        plain = client.findInstance("example", null);
        Assert.notNull(plain);

        plain = client.findInstance("example", plain.getMd5());
        Assert.isNull(plain);
    }
}
