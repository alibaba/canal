package com.alibaba.otter.canal.common.utils;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zebin.xuzb 2012-9-20 下午3:47:47
 * @version 1.0.0
 */
public class NamedThreadFactory implements ThreadFactory {

    private static final Logger           logger                   = LoggerFactory.getLogger(NamedThreadFactory.class);
    final private static String           DEFAULT_NAME             = "canal-worker";
    final private String                  name;
    final private boolean                 daemon;
    final private ThreadGroup             group;
    final private AtomicInteger           threadNumber             = new AtomicInteger(0);
    final static UncaughtExceptionHandler uncaughtExceptionHandler = (t, e) -> {
                                                                        if (e instanceof InterruptedException
                                                                            || (e.getCause() != null && e.getCause() instanceof InterruptedException)) {
                                                                            return;
                                                                        }

                                                                        logger.error("from " + t.getName(), e);
                                                                    };

    public NamedThreadFactory(){
        this(DEFAULT_NAME, true);
    }

    public NamedThreadFactory(String name){
        this(name, true);
    }

    public NamedThreadFactory(String name, boolean daemon){
        this.name = name;
        this.daemon = daemon;
        SecurityManager s = System.getSecurityManager();
        group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
    }

    public Thread newThread(Runnable r) {
        Thread t = new Thread(group, r, name + "-" + threadNumber.getAndIncrement(), 0);
        t.setDaemon(daemon);
        if (t.getPriority() != Thread.NORM_PRIORITY) {
            t.setPriority(Thread.NORM_PRIORITY);
        }

        t.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        return t;
    }

}
