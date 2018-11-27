package com.alibaba.otter.canal.example.db;

import com.alibaba.otter.canal.example.db.mysql.MysqlClient;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.util.Assert;

public class ServiceLocator implements DisposableBean {

    private static ApplicationContext applicationContext = null;

    static {
        try {
            applicationContext = new ClassPathXmlApplicationContext("classpath:client-spring.xml");
        } catch (RuntimeException e) {
            throw e;
        }
    }

    private static <T> T getBean(String name) {
        assertContextInjected();
        return (T) applicationContext.getBean(name);
    }


    private static void clearHolder() {
        ServiceLocator.applicationContext = null;
    }

    @Override
    public void destroy() throws Exception {
        ServiceLocator.clearHolder();
    }

    private static void assertContextInjected() {
        Assert.state(applicationContext != null, "ApplicationContext not set");
    }


    public static MysqlClient getMysqlClient() {
        return getBean("mysqlClient");
    }
}
