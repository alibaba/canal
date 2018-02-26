package com.alibaba.otter.canal.parse.driver.mysql.socket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;

import java.io.IOException;
import java.net.SocketAddress;

/**
 * 封装netty的通信channel和数据接收缓存，实现读、写、连接校验的功能。 2016-12-28
 * 
 * @author luoyaogui
 */
public class SocketChannel {

    private static final int WAIT_PERIOD  = 10; // milliseconds
    private static final int DEFAULT_INIT_BUFFER_SIZE = 1024 * 1024; // 1MB，默认初始缓存大小
    private static final int DEFAULT_MAX_BUFFER_SIZE = 4 * DEFAULT_INIT_BUFFER_SIZE; // 4MB，默认最大缓存大小
    private Channel channel = null;
    private Object  lock    = new Object();
    private ByteBuf cache   = PooledByteBufAllocator.DEFAULT.directBuffer(DEFAULT_INIT_BUFFER_SIZE); // 缓存大小

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public void writeCache(ByteBuf buf) throws InterruptedException, IOException {
        synchronized (lock) {
            while (true) {
                if (null == cache) {
                    throw new IOException("socket is closed !");
                }

                cache.discardReadBytes();// 回收内存
                // source buffer is empty.
                if (!buf.isReadable()) {
                    break;
                }

                if (cache.isWritable()) {
                    cache.writeBytes(buf, Math.min(cache.writableBytes(), buf.readableBytes()));
                } else {
                    // dest buffer is full.
                    lock.wait(WAIT_PERIOD);
                }
            }
        }
    }

    public void writeChannel(byte[]... buf) throws IOException {
        if (channel != null && channel.isWritable()) {
            channel.writeAndFlush(Unpooled.copiedBuffer(buf));
        } else {
            throw new IOException("write  failed  !  please checking !");
        }
    }

    public byte[] read(int readSize) throws IOException {
        return read(readSize, 0);
    }

    public byte[] read(int readSize, int timeout) throws IOException {
        int accumulatedWaitTime = 0;
        
        // 若读取内容较长，则自动扩充超时时间，以初始缓存大小为基准计算倍数
        if (timeout > 0 && readSize > DEFAULT_INIT_BUFFER_SIZE ) {
            timeout *= (readSize / DEFAULT_INIT_BUFFER_SIZE + 1);
        }
        do {
            if (readSize > cache.readableBytes()) {
                if (null == channel) {
                    throw new IOException("socket has Interrupted !");
                }

                // 默认缓存大小不够用时需自动扩充，否则将因缓存空间不足而造成I/O超时假象
                if (!cache.isWritable(readSize - cache.readableBytes())) {
                    synchronized (lock) {
                        int deltaSize = readSize - cache.readableBytes(); // 同步锁后重新读取
                        deltaSize = deltaSize - cache.writableBytes();
                        if (deltaSize > 0) {
                            deltaSize = (deltaSize / 32 + 1) * 32;
                            cache.capacity(cache.capacity() + deltaSize);
                        }
                    }
                } else if (timeout > 0) {
                    accumulatedWaitTime += WAIT_PERIOD;
                    if (accumulatedWaitTime > timeout) {
                        StringBuilder sb = new StringBuilder("socket read timeout occured !");
                        sb.append(" readSize = ").append(readSize);
                        sb.append(", readableBytes = ").append(cache.readableBytes());
                        sb.append(", timeout = ").append(timeout);
                        throw new IOException(sb.toString());
                    }
                }

                synchronized (this) {
                    try {
                        wait(WAIT_PERIOD);
                    } catch (InterruptedException e) {
                        throw new IOException("socket has Interrupted !");
                    }
                }
            } else {
                byte[] back = new byte[readSize];
                synchronized (lock) {
                    cache.readBytes(back);
                    // 恢复自动扩充的过大缓存到默认初始大小，释放空间
                    if (cache.capacity() > DEFAULT_MAX_BUFFER_SIZE) {
                        cache.discardReadBytes(); // 回收已读空间，重置读写指针
                        if (cache.readableBytes() < DEFAULT_INIT_BUFFER_SIZE) {
                            cache.capacity(DEFAULT_INIT_BUFFER_SIZE);
                        }
                    }
                }
                return back;
            }
        } while (true);
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
