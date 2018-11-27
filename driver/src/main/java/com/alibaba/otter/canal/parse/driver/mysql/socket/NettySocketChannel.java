package com.alibaba.otter.canal.parse.driver.mysql.socket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.util.internal.OutOfDirectMemoryError;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.SystemPropertyUtil;

import java.io.IOException;
import java.net.SocketAddress;

import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 封装netty的通信channel和数据接收缓存，实现读、写、连接校验的功能。 2016-12-28
 * 
 * @author luoyaogui
 */
public class NettySocketChannel implements SocketChannel {

    private static final Logger logger                   = LoggerFactory.getLogger(SocketChannel.class);
    private static final int    WAIT_PERIOD              = 10;                                                                   // milliseconds
    private static final int    DEFAULT_INIT_BUFFER_SIZE = 1024 * 1024;                                                          // 1MB，默认初始缓存大小
    // 参考 mysql-connector-java-5.1.40.jar: com.mysql.jdbc.MysqlIO.maxThreeBytes
    // < 256 * 256 * 256 = 16MB
    private static final int    DEFAULT_MAX_BUFFER_SIZE  = 16 * DEFAULT_INIT_BUFFER_SIZE;                                        // 16MB，默认最大缓存大小
    private Channel             channel                  = null;
    private Object              lock                     = new Object();
    private ByteBuf             cache                    = PooledByteBufAllocator.DEFAULT.directBuffer(DEFAULT_INIT_BUFFER_SIZE); // 缓存大小
    private int                 maxDirectBuffer          = cache.maxCapacity();

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

                // source buffer is empty.
                if (!buf.isReadable()) {
                    break;
                }

                // 默认缓存大小不够用时需自动清理或扩充，否则将因缓存空间不足而造成I/O超时假象
                int length = buf.readableBytes();
                int deltaSize = length - cache.writableBytes();
                if (deltaSize > 0) {
                    // 首先避免频繁分配内存（扩容/收缩），其次避免频繁移动内存（清理）
                    if (cache.readerIndex() >= deltaSize) { // 可以清理
                        // 回收已读空间，重置读写指针
                        cache.discardReadBytes();
                        // 恢复自动扩充的过大缓存到默认初始缓存大小，释放空间
                        int oldCapacity = cache.capacity();
                        if (oldCapacity > DEFAULT_MAX_BUFFER_SIZE) { // 尝试收缩
                            int newCapacity = cache.writerIndex();
                            newCapacity = ((newCapacity - 1) / DEFAULT_INIT_BUFFER_SIZE + 1) * DEFAULT_INIT_BUFFER_SIZE; // 对齐
                            int quarter = (newCapacity >> 2); // 至少留空四分之一
                            quarter = ((quarter - 1) / DEFAULT_INIT_BUFFER_SIZE + 1) * DEFAULT_INIT_BUFFER_SIZE; // 对齐
                            newCapacity += quarter; // 留空四分之一
                            if (newCapacity < (oldCapacity >> 1)) { // 至少收缩二分之一
                                try {
                                    cache.capacity(newCapacity);
                                    logger.info("shrink cache capacity: {} - {} = {} bytes",
                                        oldCapacity,
                                        oldCapacity - newCapacity,
                                        newCapacity);
                                } catch (OutOfMemoryError ignore) {
                                    maxDirectBuffer = oldCapacity; // 未来不再超过当前容量，记录日志后继续
                                    logger.warn("cache OutOfMemoryError: {} bytes", newCapacity, ignore);
                                }
                            }
                        }
                    } else { // 尝试扩容
                        int oldCapacity = cache.capacity();
                        if (oldCapacity < maxDirectBuffer) {
                            int quarter = (oldCapacity >> 2); // 至少扩容四分之一
                            quarter = ((quarter - 1) / DEFAULT_INIT_BUFFER_SIZE + 1) * DEFAULT_INIT_BUFFER_SIZE; // 对齐
                            deltaSize = ((deltaSize - 1) / quarter + 1) * quarter; // 对齐
                            int newCapacity = oldCapacity + deltaSize;
                            if (newCapacity > maxDirectBuffer) {
                                newCapacity = maxDirectBuffer;
                            }
                            try {
                                cache.capacity(newCapacity);
                                logger.info("expand cache capacity: {} + {} = {} bytes",
                                    oldCapacity,
                                    newCapacity - oldCapacity,
                                    newCapacity);
                            } catch (OutOfDirectMemoryError e) {
                                // failed to allocate 885571168 byte(s) of
                                // direct memory (used: 1002946176, max:
                                // 1888485376)
                                long maxDirectMemory = SystemPropertyUtil.getLong("io.netty.maxDirectMemory", -1);
                                if (maxDirectMemory < 0) {
                                    maxDirectMemory = PlatformDependent.maxDirectMemory();
                                }
                                if (maxDirectBuffer > maxDirectMemory) {
                                    maxDirectBuffer = (int) maxDirectMemory;
                                    newCapacity = maxDirectBuffer;
                                    logger.warn("resize maxDirectBuffer: {} bytes", maxDirectBuffer, e);
                                    try {
                                        cache.capacity(newCapacity);
                                        logger.info("expand cache capacity: {} + {} = {} bytes",
                                            oldCapacity,
                                            newCapacity - oldCapacity,
                                            newCapacity);
                                    } catch (OutOfMemoryError ignore) {
                                        maxDirectBuffer = oldCapacity; // 未来不再超过当前容量，记录日志后继续
                                        logger.warn("cache OutOfMemoryError: {} bytes", newCapacity, ignore);
                                    }
                                } else {
                                    maxDirectBuffer = oldCapacity; // 未来不再超过当前容量，记录日志后继续
                                    logger.warn("cache OutOfDirectMemoryError: {} bytes", newCapacity, e);
                                }
                            } catch (OutOfMemoryError ignore) {
                                maxDirectBuffer = oldCapacity; // 未来不再超过当前容量，记录日志后继续
                                logger.warn("cache OutOfMemoryError: {} bytes", newCapacity, ignore);
                            }
                        }
                    }
                    deltaSize = length - cache.writableBytes();
                }

                if (deltaSize != length) {
                    // deltaSize <= 0 可全部写入，deltaSize > 0 只能部分写入
                    if (deltaSize <= 0) {
                        cache.writeBytes(buf, length);
                        break;
                    } else {
                        cache.writeBytes(buf, length - deltaSize);
                    }
                }
                // dest buffer is full.
                lock.wait(WAIT_PERIOD);
                // 回收已读空间，重置读写指针
                cache.discardReadBytes();
            }
        }
    }

    public void write(byte[]... buf) throws IOException {
        if (channel != null && channel.isWritable()) {
            channel.writeAndFlush(Unpooled.copiedBuffer(buf));
        } else {
            throw new IOException("write failed ! please checking !");
        }
    }

    public byte[] read(int readSize) throws IOException {
        return read(readSize, 0);
    }

    public byte[] read(int readSize, int timeout) throws IOException {
        int accumulatedWaitTime = 0;

        // 若读取内容较长，则自动扩充超时时间，以初始缓存大小为基准计算倍数
        if (timeout > 0 && readSize > DEFAULT_INIT_BUFFER_SIZE) {
            timeout *= (readSize / DEFAULT_INIT_BUFFER_SIZE + 1);
        }
        do {
            if (readSize > cache.readableBytes()) {
                if (null == channel) {
                    throw new IOException("socket has Interrupted !");
                }

                if (timeout > 0) {
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
                }
                return back;
            }
        } while (true);
    }

    @Override
    public void read(byte[] data, int off, int len, int timeout) throws IOException {
        throw new NotImplementedException();
    }

    public boolean isConnected() {
        return channel != null ? true : false;
    }

    public SocketAddress getRemoteSocketAddress() {
        return channel != null ? channel.remoteAddress() : null;
    }

    public SocketAddress getLocalSocketAddress() {
        return channel != null ? channel.localAddress() : null;
    }

    public void close() {
        if (channel != null) {
            channel.close();
        }
        channel = null;
        // A fatal error has been detected by the Java Runtime Environment:
        // EXCEPTION_ACCESS_VIOLATION (0xc0000005)
        synchronized (lock) {
            cache.discardReadBytes();// 回收已占用的内存
            cache.release();// 释放整个内存
            cache = null;
        }
    }


}
