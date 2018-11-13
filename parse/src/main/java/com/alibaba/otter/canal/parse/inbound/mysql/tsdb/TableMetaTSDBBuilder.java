package com.alibaba.otter.canal.parse.inbound.mysql.tsdb;

import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.google.common.collect.Maps;

/**
 * tableMeta构造器
 * 
 * @author agapple 2018年8月8日 上午11:01:08
 * @since 1.0.25
 */

public class TableMetaTSDBBuilder {

    protected final static Logger                                        logger   = LoggerFactory.getLogger(DefaultTableMetaTSDBFactory.class);
    private static ConcurrentMap<String, ClassPathXmlApplicationContext> contexts = Maps.newConcurrentMap();

    /**
     * 代理一下tableMetaTSDB的获取,使用隔离的spring定义
     */
    public static TableMetaTSDB build(String destination, String springXml) {
        if (StringUtils.isNotEmpty(springXml)) {
            ClassPathXmlApplicationContext applicationContext = contexts.get(destination);
            if (applicationContext == null) {
                synchronized (contexts) {
                    if (applicationContext == null) {
                        applicationContext = new ClassPathXmlApplicationContext(springXml);
                        contexts.put(destination, applicationContext);
                    }
                }
            }
            TableMetaTSDB tableMetaTSDB = (TableMetaTSDB) applicationContext.getBean("tableMetaTSDB");
            logger.info("{} init TableMetaTSDB with {}", destination, springXml);
            return tableMetaTSDB;
        } else {
            return null;
        }
    }

    public static void destory(String destination) {
        ClassPathXmlApplicationContext context = contexts.remove(destination);
        if (context != null) {
            logger.info("{} destory TableMetaTSDB", destination);
            context.close();
        }
    }
}
