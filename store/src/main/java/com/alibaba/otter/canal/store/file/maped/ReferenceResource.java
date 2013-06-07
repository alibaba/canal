package com.alibaba.otter.canal.store.file.maped;

import java.util.concurrent.atomic.AtomicLong;

public abstract class ReferenceResource {

    protected final AtomicLong refCount               = new AtomicLong(1);
    protected volatile boolean available              = true;
    protected volatile boolean cleanupOver            = false;
    private volatile long      firstShutdownTimestamp = 0;

    /**
     * 资源是否能HOLD住
     */
    public synchronized boolean hold() {
        if (this.isAvailable()) {
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            } else {
                this.refCount.getAndDecrement();
            }
        }

        return false;
    }

    /**
     * 释放资源
     */
    public void release() {
        long value = this.refCount.decrementAndGet();
        if (value > 0) {
            return;
        }

        synchronized (this) {
            // cleanup内部要对是否clean做处理
            this.cleanupOver = this.cleanup(value);
        }
    }

    /**
     * 禁止资源被访问 shutdown不允许调用多次，最好是由管理线程调用
     */
    public void shutdown(final long intervalForcibly) {
        if (this.available) {
            this.available = false;
            this.firstShutdownTimestamp = System.currentTimeMillis();
            this.release();
        }
        // 强制shutdown
        else if (this.getRefCount() > 0) {
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
                this.refCount.set(-1000 - this.getRefCount());
                this.release();
            }
        }
    }

    /**
     * 资源是否可用，即是否可被HOLD
     */
    public boolean isAvailable() {
        return this.available;
    }

    /**
     * 资源是否被清理完成
     */
    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    public abstract boolean cleanup(final long currentRef);
}
