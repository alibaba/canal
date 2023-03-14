package com.alibaba.otter.canal.common;

import com.alibaba.otter.canal.common.utils.JsonUtils;
import org.junit.Test;

import java.net.InetAddress;

import static org.junit.Assert.assertEquals;

public class JsonUtilsTest {
    @Test
    public void marshalToString() throws Exception {
        InetAddress address = InetAddress.getByName("localhost");

        String json = JsonUtils.marshalToString(address);
        assertEquals("\"localhost\"", json);

        InetAddress address1 = JsonUtils.unmarshalFromString(json, InetAddress.class);
        assertEquals(address, address1);
    }
}
