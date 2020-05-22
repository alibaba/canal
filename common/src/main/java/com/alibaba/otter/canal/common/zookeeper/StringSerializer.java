package com.alibaba.otter.canal.common.zookeeper;

import java.nio.charset.StandardCharsets;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

/**
 * 基于string的序列化方式
 * 
 * @author jianghang 2012-7-11 下午02:57:09
 * @version 1.0.0
 */
public class StringSerializer implements ZkSerializer {

    public Object deserialize(final byte[] bytes) throws ZkMarshallingError {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public byte[] serialize(final Object data) throws ZkMarshallingError {
        return ((String) data).getBytes(StandardCharsets.UTF_8);
    }

}
