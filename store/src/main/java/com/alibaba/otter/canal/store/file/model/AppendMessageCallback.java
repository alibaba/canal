package com.alibaba.otter.canal.store.file.model;

import java.nio.ByteBuffer;

public interface AppendMessageCallback {

    /**
     * 序列化消息后，写入MapedByteBuffer
     * 
     * @param byteBuffer 要写入的target
     * @param maxBlank 要写入的target最大空白区
     * @param msg 要写入的message
     * @return 写入多少字节
     */
    public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
                                        final Object msg);
}
