package com.alibaba.otter.canal.client;

import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import org.junit.Assert;
import org.junit.Test;

public class CanalMessageDeserializerTest {
    @Test(expected = CanalClientException.class)
    public void testDeserializer() {
        String data = "test";
        Message message = CanalMessageDeserializer.deserializer(data.getBytes());
        System.out.println(message);
        Assert.assertTrue(message.getId() != 0);
    }
}
