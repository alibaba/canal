package com.alibaba.otter.canal.store.file;

/**
 * 后台服务线程基类
 */
public abstract class ServiceThread implements Runnable {
    // 执行线程
    protected final Thread thread;
    // 线程回收时间，默认60S
    private static final long JoinTime = 60 * 1000;
    // 是否已经被Notify过
    protected volatile boolean hasNotified = false;
    // 线程是否已经停止
    protected volatile boolean stoped = false;


    public ServiceThread() {
        this.thread = new Thread(this, this.getServiceName());
    }


    public abstract String getServiceName();


    public void start() {
        this.thread.start();
    }


    public void shutdown() {
        this.stoped = true;
        synchronized (this) {
            if (!this.hasNotified) {
                this.hasNotified = true;
                this.notify();
            }
        }

        try {
            this.thread.join(this.getJointime());
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public void wakeup() {
        synchronized (this) {
            if (!this.hasNotified) {
                this.hasNotified = true;
                this.notify();
            }
        }
    }


    protected void waitForRunning(long interval) {
        synchronized (this) {
            if (this.hasNotified) {
                this.hasNotified = false;
                this.onWaitEnd();
                return;
            }

            try {
                this.wait(interval);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            finally {
                this.hasNotified = false;
                this.onWaitEnd();
            }
        }
    }


    protected void onWaitEnd() {
    }


    public boolean isStoped() {
        return stoped;
    }


    public long getJointime() {
        return JoinTime;
    }
}
