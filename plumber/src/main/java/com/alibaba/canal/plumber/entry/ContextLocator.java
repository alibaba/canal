package com.alibaba.canal.plumber.entry;

import com.alibaba.canal.plumber.stage.StageController;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;


/**
 * context上下文
 * @author dsqin
 * @date 2018/6/5
 */
public class ContextLocator {

    private static ClassPathXmlApplicationContext context = null;
    private static RuntimeException initException = null;

    public ContextLocator() {
    }

    private static ApplicationContext getApplicationContext() {
        if (context == null) {
            throw initException;
        } else {
            return context;
        }
    }

    public static void close() {
        context.close();
    }

    public static StageController getController() {
        return getApplicationContext().getBean(StageController.class);
    }

    static {
        try {
            context = new ClassPathXmlApplicationContext("applicationContext.xml") {
                protected void customizeBeanFactory(DefaultListableBeanFactory beanFactory) {
                    super.customizeBeanFactory(beanFactory);
                    beanFactory.setAllowBeanDefinitionOverriding(false);
                }
            };
        } catch (RuntimeException var1) {
            throw var1;
        }
    }
}
