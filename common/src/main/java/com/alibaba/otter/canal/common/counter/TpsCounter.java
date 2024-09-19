package com.alibaba.otter.canal.common.counter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * tps计数器
 * 当tps超过阈值时，等待阻塞，直到恢复tps
 */
public class TpsCounter {

    /**
     * 缓存tps实例
     */
    private static final Map<String, TpsCounter> TPS_COUNTERS = new java.util.concurrent.ConcurrentHashMap<>();

    private static Logger logger = LoggerFactory.getLogger(TpsCounter.class);

    /**
     * 初始tps计数为0
     */
    private int currentTps = 0;
    /**
     * 最后一次更新时间
     */
    private long lastTimestamp = -1L;

    /**
     * 将构造私有化
     */
    private TpsCounter(){}

    /**
     * 双重校验锁, 和同步HashMap保证同一名称的实例唯一性
     * @param name 名称
     * @return tpsCounter实例
     */
    public static TpsCounter getInstance(String name){
        TpsCounter tpsCounter = TPS_COUNTERS.get(name);
        if (tpsCounter == null){
            synchronized (TpsCounter.class){
                tpsCounter = TPS_COUNTERS.computeIfAbsent(name, k -> new TpsCounter());
            }
        }
        return tpsCounter;
    }

    /**
     * 同步方法返回当前的tps计数
     * @return tps
     */
    public synchronized int getCurrentTps(){
        if (lastTimestamp == -1L){
            lastTimestamp = timeGen();
        }
        if (timeGen() == lastTimestamp){
            currentTps++;
        } else {
            lastTimestamp = timeGen();
            currentTps = 1;
        }
        return currentTps;
    }

    /**
     * 当前tps是否超过最大tps
     * @param maxTps 最大tps
     * @return true: 超过, false: 未超过
     */
    public synchronized boolean isOverTps(int maxTps){
        return getCurrentTps() > maxTps;
    }

    /**
     * 等待tps窗口, 如果在当前tps已超过最大tps, 则等待直到下一个时间窗口
     * @param maxTps 最大tps
     */
    public synchronized void waitTps(int maxTps){
        logger.info("current time {}, time:{}, current req count {}", timeGen(), timeGen(),this.currentTps);
        while (isOverTps(maxTps)){
            try {
                long waitTime = 1000L - System.currentTimeMillis() % 1000L + 2L;
                logger.info("tps over {}, sleep {}ms, current time {}, current req count {}", maxTps,  waitTime, timeGen(), this.currentTps);
                Thread.sleep(waitTime);
                logger.info("current time {}, current req count {}", timeGen(), this.currentTps);
            } catch (Throwable ignore) {
            }
        }
    }

    /**
     * 获取当前时间的秒数
     * @return 秒
     */
    private long timeGen(){
        return System.currentTimeMillis() / 1000;
    }

}
