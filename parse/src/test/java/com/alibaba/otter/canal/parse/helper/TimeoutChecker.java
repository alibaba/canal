package com.alibaba.otter.canal.parse.helper;

/**
 * 用于检查超时, 主要用于启动服务以后，如果在指定的时间内没有响应，则自动退出
 * 
 * @author: yuanzu Date: 12-9-26 Time: 上午10:55
 */
public class TimeoutChecker {

    /**
     * 最后一次动作的时间
     */
    private long              lastTouch;
    /**
     * 超时时间
     */
    private long              timeoutMillis;

    /**
     * 运行标志
     */
    private boolean           running                = true;

    /**
     * default 3s
     */
    private static final long DEFAULT_TIMEOUT_MILLIS = 3 * 1000;

    public TimeoutChecker(long timeoutMillis){
        this.timeoutMillis = timeoutMillis;
        touch();
    }

    public TimeoutChecker(){
        this(DEFAULT_TIMEOUT_MILLIS);
    }

    /**
     * 更新
     */
    public void touch() {
        this.lastTouch = System.currentTimeMillis();
    }

    /**
     * 等待空闲
     * 
     * @throws InterruptedException
     */
    public void waitForIdle() throws InterruptedException {
        while (this.running && (System.currentTimeMillis() - this.lastTouch) < this.timeoutMillis) {
            Thread.sleep(50);
        }
    }

    public void stop() {
        this.running = false;
    }
}
