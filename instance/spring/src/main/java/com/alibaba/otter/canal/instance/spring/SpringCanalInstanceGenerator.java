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
