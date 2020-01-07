package com.alibaba.otter.canal.common;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.alibaba.otter.canal.common.utils.NamedThreadFactory;
import com.alibaba.otter.canal.spi.CanalMQProducer;

/**
 * @author agapple 2019年9月29日 上午11:17:11
 * @since 1.1.5
 */
public abstract class AbstractMQProducer implements CanalMQProducer {

    protected ThreadPoolExecutor executor;

    @Override
    public void init(MQProperties mqProperties) {
        int parallelThreadSize = mqProperties.getParallelThreadSize();
        executor = new ThreadPoolExecutor(parallelThreadSize,
            parallelThreadSize,
            0,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(parallelThreadSize * 2),
            new NamedThreadFactory("MQParallel"),
            new ThreadPoolExecutor.CallerRunsPolicy());

    }

    @Override
    public void stop() {
        executor.shutdownNow();
        executor = null;
    }

}
