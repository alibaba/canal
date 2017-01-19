package com.alibaba.otter.canal.parse.driver.mysql.socket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

/**
 * 封装netty的通信channel和数据接收缓存，实现读、写、连接校验的功能。 2016-12-28
 * 
 * @author luoyaogui
 */
public class SocketChannel {

    private Channel channel = null;
    private Object  lock    = new Object();
    private ByteBuf cache   = PooledByteBufAllocator.DEFAULT.directBuffer(1024 * 1024 * 5); // 缓存大小

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel, boolean notify) {
        this.channel = channel;
        if (notify) {// 是否需要通知，主要是channel不可用时
            synchronized (this) {
                notifyAll();
            }
        }
    }

    public void writeCache(ByteBuf buf) {
        synchronized (lock) {
            cache.discardReadBytes();// 回收内存
            cache.writeBytes(buf);
        }
        synchronized (this) {
            notifyAll();
        }
    }

    public void writeChannel(byte[]... buf) throws IOException {
        if (channel != null && channel.isWritable()) {
            channel.writeAndFlush(Unpooled.copiedBuffer(buf));
        } else {
            throw new IOException("write  failed  !  please checking !");
        }
    }

    public int read(ByteBuffer buffer) throws IOException {
        if (null == channel) {
            throw new IOException("socket has Interrupted !");
        }
        if (cache.readableBytes() < buffer.remaining()) {
            synchronized (this) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    throw new IOException("socket has Interrupted !");
                }
            }
        } else {
            synchronized (lock) {
                cache.readBytes(buffer);
            }
        }
        return 0;
    }

    public boolean isConnected() {
        return channel != null ? true : false;
    }

    public SocketAddress getRemoteSocketAddress() {
        return channel != null ? channel.remoteAddress() : null;
    }

    public void close() {
        if (channel != null) {
            channel.close();
        }
        channel = null;
        cache.discardReadBytes();// 回收已占用的内存
        cache.release();// 释放整个内存
        cache = null;
    }
}
