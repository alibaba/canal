package com.alibaba.otter.canal.admin.common;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Threads {

    public static int                   pool            = 60;
    public static final ExecutorService executorService = new ThreadPoolExecutor(pool,
                                                            pool,
                                                            0L,
                                                            TimeUnit.MILLISECONDS,
                                                            new ArrayBlockingQueue<Runnable>(pool * 20),
                                                            DaemonThreadFactory.daemonThreadFactory);
}
