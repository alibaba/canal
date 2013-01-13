/**
 * Project: otter.canal.server-1.0.0
 * 
 * File Created at 2012-7-12
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
package com.alibaba.otter.canal.instance.spring;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;

import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.instance.core.CanalInstanceGenerator;

/**
 * @author zebin.xuzb @ 2012-7-12
 * @version 1.0.0
 */
public class SpringCanalInstanceGenerator implements CanalInstanceGenerator, BeanFactoryAware {

    private String      defaultName = "instance";
    private BeanFactory beanFactory;

    public CanalInstance generate(String destination) {
        String beanName = destination;
        if (!beanFactory.containsBean(beanName)) {
            beanName = defaultName;
        }

        return (CanalInstance) beanFactory.getBean(beanName);
    }

    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = beanFactory;
    }

}
