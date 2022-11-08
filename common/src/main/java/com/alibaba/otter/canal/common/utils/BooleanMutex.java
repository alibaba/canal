package com.alibaba.otter.canal.common.utils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * 实现一个互斥实现，基于Cocurrent中的AQS实现了自己的sync <br/>
 * 应用场景：系统初始化/授权控制，没权限时阻塞等待。有权限时所有线程都可以快速通过
 * 
 * <pre>
 * false : 代表需要被阻塞挂起，等待mutex变为true被唤醒
 * true : 唤醒被阻塞在false状态下的thread
 * 
 * BooleanMutex mutex = new BooleanMutex(true);
 * try {
 *     mutex.get(); //当前状态为true, 不会被阻塞
 * } catch (InterruptedException e) {
 *     // do something
 * }
 * 
 * mutex.set(false);
 * try {
 *     mutex.get(); //当前状态为false, 会被阻塞直到另一个线程调用mutex.set(true);
 * } catch (InterruptedException e) {
 *     // do something
 * }
 * </pre>
 * 
 * @author jianghang 2011-9-23 上午09:58:03
 * @version 1.0.0
 */
public class BooleanMutex {

    private final Sync sync;

    public BooleanMutex(){
        sync = new Sync();
        set(false);
    }

    public BooleanMutex(Boolean mutex){
        sync = new Sync();
        set(mutex);
    }

    /**
     * 阻塞等待Boolean为true
     * 
     * @throws InterruptedException if the current thread is interrupted
     */
    public void get() throws InterruptedException {
        sync.innerGet();
    }

    /**
     * 阻塞等待Boolean为true,允许设置超时时间
     * 
     * @param timeout
     * @param unit
     * @throws InterruptedException
     * @throws TimeoutException
     */
    public void get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
        sync.innerGet(unit.toNanos(timeout));
    }

    /**
     * 重新设置对应的Boolean mutex
     * 
     * @param mutex
     */
    public void set(Boolean mutex) {
        if (mutex) {
            sync.innerSetTrue();
        } else {
            sync.innerSetFalse();
        }
    }

    public boolean state() {
        return sync.innerState();
    }

    /**
     * Synchronization control for BooleanMutex. Uses AQS sync state to
     * represent run status
     */
    private static final class Sync extends AbstractQueuedSynchronizer {

        private static final long serialVersionUID = 2559471934544126329L;
        /** State value representing that TRUE */
        private static final int  TRUE             = 1;
        /** State value representing that FALSE */
        private static final int  FALSE            = 2;

        private boolean isTrue(int state) {
            return (state & TRUE) != 0;
        }

        /**
         * 实现AQS的接口，获取共享锁的判断
         */
        protected int tryAcquireShared(int state) {
            // 如果为true，直接允许获取锁对象
            // 如果为false，进入阻塞队列，等待被唤醒
            return isTrue(getState()) ? 1 : -1;
        }

        /**
         * 实现AQS的接口，释放共享锁的判断
         */
        protected boolean tryReleaseShared(int ignore) {
            // 始终返回true，代表可以release
            return true;
        }

        boolean innerState() {
            return isTrue(getState());
        }

        void innerGet() throws InterruptedException {
            acquireSharedInterruptibly(0);
        }

        void innerGet(long nanosTimeout) throws InterruptedException, TimeoutException {
            if (!tryAcquireSharedNanos(0, nanosTimeout)) throw new TimeoutException();
        }

        void innerSetTrue() {
            for (;;) {
                int s = getState();
                if (s == TRUE) {
                    return; // 直接退出
                }
                if (compareAndSetState(s, TRUE)) {// cas更新状态，避免并发更新true操作
                    releaseShared(0);// 释放一下锁对象，唤醒一下阻塞的Thread
                    return;
                }
            }
        }

        void innerSetFalse() {
            for (;;) {
                int s = getState();
                if (s == FALSE) {
                    return; // 直接退出
                }
                if (compareAndSetState(s, FALSE)) {// cas更新状态，避免并发更新false操作
                    return;
                }
            }
        }

    }

}
