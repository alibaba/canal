package com.alibaba.otter.canal.connector.core.spi;

import com.alibaba.otter.canal.connector.core.config.MQProperties;
import com.alibaba.otter.canal.connector.core.producer.MQDestination;
import com.alibaba.otter.canal.connector.core.util.Callback;
import com.alibaba.otter.canal.protocol.Message;
import java.util.Properties;

public class ProxyCanalMQProducer implements CanalMQProducer {

    private CanalMQProducer canalMQProducer;

    public ProxyCanalMQProducer(CanalMQProducer canalMQProducer) {
        this.canalMQProducer = canalMQProducer;
    }

    private ClassLoader changeCL() {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(canalMQProducer.getClass().getClassLoader());
        return cl;
    }

    private void revertCL(ClassLoader cl) {
        Thread.currentThread().setContextClassLoader(cl);
    }

    @Override
    public void init(Properties properties) {
        ClassLoader cl = changeCL();
        try {
            canalMQProducer.init(properties);
        } finally {
            revertCL(cl);
        }
    }

    @Override
    public MQProperties getMqProperties() {
        ClassLoader cl = changeCL();
        try {
            return canalMQProducer.getMqProperties();
        } finally {
            revertCL(cl);
        }
    }

    @Override
    public void send(MQDestination canalDestination, Message message, Callback callback) {
        ClassLoader cl = changeCL();
        try {
            canalMQProducer.send(canalDestination, message, callback);
        } finally {
            revertCL(cl);
        }
    }

    @Override
    public void stop() {
        ClassLoader cl = changeCL();
        try {
            canalMQProducer.stop();
        } finally {
            revertCL(cl);
        }
    }
}
