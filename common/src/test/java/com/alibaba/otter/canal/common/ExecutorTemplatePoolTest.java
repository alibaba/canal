package com.alibaba.otter.canal.common;

import com.alibaba.otter.canal.common.utils.ExecutorTemplate;
import com.alibaba.otter.canal.common.utils.ExecutorTemplatePool;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class ExecutorTemplatePoolTest {

    ExecutorTemplatePool executorTemplatePool = new ExecutorTemplatePool();

    @Test
    public void baseTest(){
        System.out.println("Begin!");
        AtomicInteger sum = new AtomicInteger(0);
        ExecutorTemplate executorTemplate = executorTemplatePool.getExecutorTemplate("root");
        for (int i=0; i<100; i++){
            final int _i = i;
            executorTemplate.submit(new Runnable() {
                @Override
                public void run() {
                    int n = sum.incrementAndGet();
//          System.out.println("i="+_i+", n="+n);
                }
            });
        }
        executorTemplate.waitForResult();
        System.out.println("sum="+sum.get());
        executorTemplatePool.close();
        System.out.println("End!");
    }

    @Test
    public void multiLevelTest(){
        System.out.println("Begin!");
        AtomicInteger rootSum = new AtomicInteger(0);
        AtomicInteger subSum = new AtomicInteger(0);

        ExecutorTemplate executorTemplate = executorTemplatePool.getExecutorTemplate("root");
        for (int i=0; i<100; i++){
            final int _i = i;
            executorTemplate.submit(() -> {
                int n = rootSum.incrementAndGet();
//        System.out.println("i=" + _i + ", n=" + n);

                ExecutorTemplate executorTemplate2 = executorTemplatePool.getExecutorTemplate("sub");
                for (int j = 0; j < 10; j++) {
                    final int _j = j;
                    executorTemplate2.submit(() -> {
                        int m = subSum.incrementAndGet();
//            System.out.println("[sub] j=" + _j + ", m=" + m);
                    });
                }
                executorTemplate2.waitForResult();
            });
        }
        executorTemplate.waitForResult();
        System.out.println("rootSum="+rootSum.get()+", subSum="+subSum.get());
        executorTemplatePool.close();
        System.out.println("End!");
    }
}
