package com.alibaba.otter.canal.instance.spring.integrated;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.util.Assert;

import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.instance.core.CanalInstanceGenerator;

/**
 * @author zebin.xuzb @ 2012-7-13
 * @version 1.0.0
 */
@Ignore
public class GroupSpringInstanceTest {

    private ApplicationContext context;

    @Before
    public void start() {
        System.setProperty("canal.instance.destination", "retl");
        context = new ClassPathXmlApplicationContext(new String[] { "spring/group-instance.xml" });
    }

    @After
    public void close() {
        if (context != null && context instanceof AbstractApplicationContext) {
            ((AbstractApplicationContext) context).close();
        }
    }

    @Test
    public void testInstance() {
        CanalInstanceGenerator generator = (CanalInstanceGenerator) context.getBean("canalInstanceGenerator");
        CanalInstance canalInstance = generator.generate("instance");
        Assert.notNull(canalInstance);

        canalInstance.start();
        try {
            Thread.sleep(10 * 1000);
        } catch (InterruptedException e) {
        }
        canalInstance.stop();
    }
}
