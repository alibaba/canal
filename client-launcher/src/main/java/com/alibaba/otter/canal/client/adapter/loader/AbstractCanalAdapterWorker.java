package com.alibaba.otter.canal.client.adapter.loader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.alibaba.otter.canal.client.adapter.CanalOuterAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.protocol.Message;

/**
 * 适配器工作线程抽象类
 *
 * @author machengyuan 2018-8-19 下午11:30:49
 * @version 1.0.0
 */
public abstract class AbstractCanalAdapterWorker {

    protected final Logger                    logger  = LoggerFactory.getLogger(this.getClass());

    protected String                          canalDestination;                                                 // canal实例
    protected List<List<CanalOuterAdapter>>   canalOuterAdapters;                                               // 外部适配器
    protected ExecutorService                 groupInnerExecutorService;                                        // 组内工作线程池
    protected volatile boolean                running = false;                                                  // 是否运行中
    protected Thread                          thread  = null;
    protected Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {

                                                          @Override
                                                          public void uncaughtException(Thread t, Throwable e) {
                                                              logger.error("parse events has an error", e);
                                                          }
                                                      };

    protected void writeOut(final Message message) {
        List<Future<Boolean>> futures = new ArrayList<>();
        // 组间适配器并行运行
        for (List<CanalOuterAdapter> outerAdapters : canalOuterAdapters) {
            final List<CanalOuterAdapter> adapters = outerAdapters;
            futures.add(groupInnerExecutorService.submit(new Callable<Boolean>() {

                @Override
                public Boolean call() {
                    try {
                        // 组内适配器穿行运行，尽量不要配置组内适配器
                        for (CanalOuterAdapter c : adapters) {
                            long begin = System.currentTimeMillis();
                            c.writeOut(message);
                            if (logger.isDebugEnabled()) {
                                logger.debug("{} elapsed time: {}",
                                    c.getClass().getName(),
                                    (System.currentTimeMillis() - begin));
                            }
                        }
                        return true;
                    } catch (Exception e) {
                        return false;
                    }
                }
            }));

            // 等待所有适配器写入完成
            // 由于是组间并发操作，所以将阻塞直到耗时最久的工作组操作完成
            for (Future<Boolean> f : futures) {
                try {
                    if (!f.get()) {
                        logger.error("Outer adapter write failed");
                    }
                } catch (InterruptedException | ExecutionException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

    protected void writeOut(Message message,String topic){
        if (logger.isDebugEnabled()) {
            logger.debug("topic: {} batchId: {} batchSize: {} ",
                topic,
                message.getId(),
                message.getEntries().size());
        }
        long begin = System.currentTimeMillis();
        writeOut(message);
        long now = System.currentTimeMillis();
        if ((System.currentTimeMillis() - begin) > 5 * 60 * 1000) {
            logger.error("topic: {} batchId {} elapsed time: {} ms",
                topic,
                message.getId(),
                now - begin);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("topic: {} batchId {} elapsed time: {} ms",
                topic,
                message.getId(),
                now - begin);
        }
    }

    protected void stopOutAdapters(){
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }
        groupInnerExecutorService.shutdown();
        logger.info("topic connectors' worker thread dead!");
        for (List<CanalOuterAdapter> outerAdapters : canalOuterAdapters) {
            for (CanalOuterAdapter adapter : outerAdapters) {
                adapter.destroy();
            }
        }
        logger.info("topic all connectors destroyed!");
    }
    public abstract void start();

    public abstract void stop();
}
