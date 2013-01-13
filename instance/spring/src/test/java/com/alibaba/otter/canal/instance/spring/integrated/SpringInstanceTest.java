/**
 * Project: otter.canal.instance.spring-1.0.0
 * 
 * File Created at 2012-7-13
 * $Id$
 * 
 * Copyright 1999-2100 Alibaba.com Corporation Limited.
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * Alibaba Company. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Alibaba.com.
 */
package com.alibaba.otter.canal.instance.spring.integrated;

import org.junit.After;
import org.junit.Before;
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
public class SpringInstanceTest {

    private ApplicationContext context;

    @Before
    public void start() {
        context = new ClassPathXmlApplicationContext(new String[] { "canal.xml" });
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
        CanalInstance canalInstance = generator.generate("retl");
        Assert.notNull(canalInstance);
    }
}
