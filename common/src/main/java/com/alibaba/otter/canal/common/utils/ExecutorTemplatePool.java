package com.alibaba.otter.canal.common.utils;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 将ExecutorTemplate和线程池一起做一层封装，为的是能支持多级业务并发（为避免嵌套等待）
 * <p>
 * 该类的名字虽是模板池，但其实并没有缓存模板对象本身。因为模板对象是轻量级的，无需缓存。 每次调用getExecutorTemplate()方法时都会new一个新的模板对象，这样还可以避免因使用不当而出现wait失效的风险。
 * <p>
 * 该类中维护了一个线程池集合，通过将name参数传入getExecutorTemplate()方法来指定模板使用哪个线程池。 若指定的线程池不存在，则会自动新建一个。
 * <p>
 * 默认情况下，新建线程池的线程数为8，排队队列为线程数的2倍。使用者可通过调用setDefaultThreadNum()方法来变更默认值。
 * 也可以在调用getExecutorTemplate()方法时通过threadNum参数指定本次新建的线程池大小（只有name不存在时才会新建）。
 * <p>
 * 注意，本模板池在使用完毕后需要调用其close方法，以释放其中的线程池资源
 */
public class ExecutorTemplatePool {

    /**
     * 默认线程池核心线程数
     */
    private int defaultThreadNum = 8;

    /**
     * 线程池集合
     */
    private Map<String, ThreadPoolExecutor> executorMap = new ConcurrentHashMap<>();

    /**
     * 变更默认线程池核心线程数 后续创建的线程池将会使用新的默认数值
     */
    public void setDefaultThreadNum(int defaultThreadNum) {
        this.defaultThreadNum = defaultThreadNum;
    }

    /**
     * 获得一个ExecutorTemplate模板对象
     *
     * @param name 指定该模板对象所对应的线程池
     */
    public ExecutorTemplate getExecutorTemplate(String name) {
        return getExecutorTemplate(name, 0);
    }

    /**
     * 获得一个ExecutorTemplate模板对象
     *
     * @param name      指定该模板对象所对应的线程池
     * @param threadNum 指定首次创建线程池时的池大小
     */
    public ExecutorTemplate getExecutorTemplate(String name, int threadNum) {
        final int _threadNum;
        if (threadNum <= 0) {
            _threadNum = defaultThreadNum;
        } else {
            _threadNum = threadNum;
        }
        ThreadPoolExecutor executor = executorMap.computeIfAbsent(name, key ->
                new ThreadPoolExecutor(_threadNum,
                        _threadNum,
                        0,
                        TimeUnit.SECONDS,
                        new ArrayBlockingQueue<Runnable>(_threadNum * 2),
                        new NamedThreadFactory("MQParallel_" + name),
                        new ThreadPoolExecutor.CallerRunsPolicy()));

        return new ExecutorTemplate(executor);

    }

    public void close() {
        if (!executorMap.isEmpty()) {
            for (ThreadPoolExecutor executor : executorMap.values()) {
                executor.shutdownNow();
            }
            executorMap.clear();
        }
    }

}
