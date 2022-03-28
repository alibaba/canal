package com.alibaba.otter.canal.common.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 多线程执行器模板代码，otter中好多地方都写多线程，比较多的都是重复的逻辑代码，抽象一下做个模板把
 *
 * <pre>
 * 示例代码：
 * ExecutorTemplate template = new ExecutorTemplate(executor);
 * ...
 * try {
 *    for ( ....) {
 *       template.submit(new Runnable() {})
 *    }
 * 
 *    List<?> result = template.waitForResult();
 *    // do result
 * } finally {
 *    template.clear();
 * }
 * 
 * 注意：该模板工程，不支持多业务并发调用，会出现数据混乱
 * </pre>
 */
public class ExecutorTemplate {

    private volatile ThreadPoolExecutor executor = null;
    private volatile List<Future>       futures  = null;

    public ExecutorTemplate(ThreadPoolExecutor executor){
        this.futures = Collections.synchronizedList(new ArrayList<>());
        this.executor = executor;
    }

    public void submit(Runnable task) {
        Future future = executor.submit(task, null);
        futures.add(future);
        check(future);
    }

    public void submit(Callable<Exception> task) {
        Future future = executor.submit(task);
        futures.add(future);
        check(future);
    }

    private void check(Future future) {
        if (future.isDone()) {
            // 立即判断一次，因为使用了CallerRun可能当场跑出结果，针对有异常时快速响应，而不是等跑完所有的才抛异常
            try {
                future.get();
            } catch (Throwable e) {
                // 取消完之后立马退出
                cacelAllFutures();
                throw new RuntimeException(e);
            }
        }
    }

    public synchronized List<?> waitForResult() {
        List result = new ArrayList();
        RuntimeException exception = null;

        for (Future future : futures) {
            try {
                result.add(future.get());
            } catch (Throwable e) {
                exception = new RuntimeException(e);
                // 如何一个future出现了异常，就退出
                break;
            }
        }

        if (exception != null) {
            cacelAllFutures();
            throw exception;
        } else {
            return result;
        }
    }

    public void cacelAllFutures() {
        for (Future future : futures) {
            if (!future.isDone() && !future.isCancelled()) {
                future.cancel(true);
            }
        }
    }

    public void clear() {
        futures.clear();
    }

}
