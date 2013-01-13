package com.alibaba.otter.canal.common.zookeeper;

import java.io.UnsupportedEncodingException;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

/**
 * 基于string的序列化方式
 * 
 * @author jianghang 2012-7-11 下午02:57:09
 * @version 1.0.0
 */
public class ByteSerializer implements ZkSerializer {

    public Object deserialize(final byte[] bytes) throws ZkMarshallingError {
        return bytes;
    }

    public byte[] serialize(final Object data) throws ZkMarshallingError {
        try {
            if (data instanceof byte[]) {
                return (byte[]) data;
            } else {
                return ((String) data).getBytes("utf-8");
            }
        } catch (final UnsupportedEncodingException e) {
            throw new ZkMarshallingError(e);
        }
    }

}
