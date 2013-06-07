package com.alibaba.otter.canal.store.file.maped;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.store.file.model.AppendMessageCallback;
import com.alibaba.otter.canal.store.file.model.AppendMessageResult;
import com.alibaba.otter.canal.store.file.model.AppendMessageStatus;
import com.alibaba.otter.canal.store.file.model.SelectMapedBufferResult;

/**
 * 对应一个具体的文件，主要参考了metaq的实现
 * 
 * @author jianghang 2013-5-24 下午09:57:39
 * @version 1.0.6
 */
public class MapedFile extends ReferenceResource {

    private static final Logger        log                    = LoggerFactory.getLogger(MapedFile.class);
    public static final int            OS_PAGE_SIZE           = 1024 * 4;                                // 4kb
    // 当前JVM中映射的虚拟内存总大小
    private static final AtomicLong    totalMapedVitualMemory = new AtomicLong(0);
    // 当前JVM中mmap句柄数量
    private static final AtomicInteger totalMapedFiles        = new AtomicInteger(0);

    // 映射的文件名
    private final String               fileName;
    // 映射的起始偏移量
    private final long                 fileFromOffset;
    // 映射的文件大小，定长
    private final int                  fileSize;
    // 映射的文件
    private final File                 file;
    // 映射的FileChannel对象
    private final FileChannel          fileChannel;
    // 映射的内存对象，position永远不变
    private final MappedByteBuffer     mappedByteBuffer;
    // 当前写到什么位置
    private final AtomicInteger        wrotePostion           = new AtomicInteger(0);
    // Flush到什么位置
    private final AtomicInteger        committedPosition      = new AtomicInteger(0);
    // 最后一条消息存储时间
    private volatile long              storeTimestamp         = 0;

    private boolean                    firstCreateInQueue     = false;

    public MapedFile(final String fileName, final int fileSize) throws IOException{
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        FileUtils.forceMkdir(this.file.getParentFile());

        try {
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            totalMapedVitualMemory.addAndGet(fileSize);
            totalMapedFiles.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("create file channel " + this.fileName + " Failed. ", e);
            throw e;
        } catch (IOException e) {
            log.error("map file " + this.fileName + " Failed. ", e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    /**
     * 向MapedFile追加消息<br>
     * 
     * @param msg 要追加的消息
     * @param cb 用来对消息进行序列化，尤其对于依赖MapedFile Offset的属性进行动态序列化
     * @return 是否成功，写入多少数据
     */
    public AppendMessageResult appendMessage(final Object msg, final AppendMessageCallback cb) {
        assert msg != null;
        assert cb != null;

        int currentPos = this.wrotePostion.get();

        // 表示有空余空间
        if (currentPos < this.fileSize) {
            ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);
            AppendMessageResult result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos,
                                                     msg);
            this.wrotePostion.addAndGet(result.getWroteBytes());
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }

        // 上层应用应该保证不会走到这里
        log.error("MapedFile.appendMessage return null, wrotePostion: {}  fileSize: {} ", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    /**
     * 消息刷盘
     * 
     * @param flushLeastPages 至少刷几个page
     * @return
     */
    public int commit(final int flushLeastPages) {
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                int value = this.wrotePostion.get();
                this.mappedByteBuffer.force();// 强制刷出数据到磁盘
                this.committedPosition.set(value);
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = {}", this.committedPosition.get());
                this.committedPosition.set(this.wrotePostion.get());
            }
        }

        return this.getCommittedPosition();
    }

    /**
     * 判断是否需要进行flush操作，如果指定了flushLeastPages，就会计算是否有满足足够的pages，一般操作系统每页大小为4KB
     */
    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = this.committedPosition.get();
        int write = this.wrotePostion.get();

        // 如果当前文件已经写满，应该立刻刷盘
        if (this.isFull()) {
            return true;
        }

        // 只有未刷盘数据满足指定page数目才刷盘
        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return write > flush;
    }

    /**
     * 读取指定位置和大小的数据块
     * 
     * @param pos
     * @param size
     * @return
     */
    public SelectMapedBufferResult selectMapedBuffer(int pos, int size) {
        // 有消息
        if ((pos + size) <= this.wrotePostion.get()) {
            // 从MapedBuffer读
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMapedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: {} , fileFromOffset: {}", pos, this.fileFromOffset);
            }
        }
        // 请求参数非法
        else {
            log.warn("selectMapedBuffer request pos invalid, request pos: {}, size: {}, fileFromOffset: {}"
                     + new Object[] { pos, size, this.fileFromOffset });
        }

        // 非法参数或者mmap资源已经被释放
        return null;
    }

    /**
     * 读取指定位置开始所有已经写入的数据
     */
    public SelectMapedBufferResult selectMapedBuffer(int pos) {
        if (pos < this.wrotePostion.get() && pos >= 0) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = this.wrotePostion.get() - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMapedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        // 非法参数或者mmap资源已经被释放
        return null;
    }

    /**
     * 清理对应的文件资源，比如释放maped内存
     */
    public boolean cleanup(final long currentRef) {
        // 如果没有被shutdown，则不可以unmap文件，否则会crash
        if (this.isAvailable()) {
            log.error("this file[REF:{}] {} have not shutdown, stop unmaping.", currentRef, this.fileName);
            return false;
        }

        // 如果已经cleanup，再次操作会引起crash
        if (this.isCleanupOver()) {
            log.error("this file[REF:{}] {} have cleanup, do not do it again.", currentRef, this.fileName);
            // 必须返回true
            return true;
        }

        MapedFileUtils.clean(this.mappedByteBuffer);// 尝试umap资源
        totalMapedVitualMemory.addAndGet(this.fileSize * (-1));
        totalMapedFiles.decrementAndGet();
        log.info("unmap file[REF:{}] {} OK", currentRef, this.fileName);
        return true;
    }

    /**
     * 清理资源，destroy与调用shutdown的线程必须是同一个
     * 
     * @return 是否被destory成功，上层调用需要对失败情况处理，失败后尝试重试
     */
    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);
        if (this.isCleanupOver()) {
            try {
                this.fileChannel.close();
                log.info("close file channel {} OK", this.fileName);

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info("delete file[REF:{}] {} {}, W:{} M:{}, {}", new Object[] { this.getRefCount(), this.fileName,
                        result ? " OK, " : " Failed, ", this.getWrotePostion(), this.getCommittedPosition(),
                        MapedFileUtils.computeEclipseTimeMilliseconds(beginTime) });
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy maped file[REF:{}] {} Failed. cleanupOver: {}", new Object[] { this.getRefCount(),
                    this.fileName, this.cleanupOver });
        }

        return false;
    }

    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public String getFileName() {
        return fileName;
    }

    /**
     * 文件起始偏移量
     */
    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    /**
     * 获取文件大小
     */
    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public boolean isFull() {
        return this.fileSize == this.wrotePostion.get();
    }

    // ================== setter / getter =========================

    public int getWrotePostion() {
        return wrotePostion.get();
    }

    public int getCommittedPosition() {
        return committedPosition.get();
    }

    public void setWrotePostion(int pos) {
        this.wrotePostion.set(pos);
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    public static int getTotalmapedfiles() {
        return totalMapedFiles.get();
    }

    public static long getTotalMapedVitualMemory() {
        return totalMapedVitualMemory.get();
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    /**
     * 方法不能在运行时调用，不安全。只在启动时，reload已有数据时调用
     */
    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }
}
