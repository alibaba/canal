package com.alibaba.otter.canal.connector.core.spi;

import com.alibaba.otter.canal.connector.core.consumer.CommonMessage;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ProxyCanalMsgConsumer implements CanalMsgConsumer {

    private CanalMsgConsumer canalMsgConsumer;

    public ProxyCanalMsgConsumer(CanalMsgConsumer canalMsgConsumer) {
        this.canalMsgConsumer = canalMsgConsumer;
    }

    private ClassLoader changeCL() {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(canalMsgConsumer.getClass().getClassLoader());
        return cl;
    }

    private void revertCL(ClassLoader cl) {
        Thread.currentThread().setContextClassLoader(cl);
    }


    @Override
    public void init(Properties properties, String topic, String groupId) {
        ClassLoader cl = changeCL();
        try {
            canalMsgConsumer.init(properties, topic, groupId);
        } finally {
            revertCL(cl);
        }
    }

    @Override
    public void connect() {
        ClassLoader cl = changeCL();
        try {
            canalMsgConsumer.connect();
        } finally {
            revertCL(cl);
        }
    }

    @Override
    public List<CommonMessage> getMessage(Long timeout, TimeUnit unit) {
        ClassLoader cl = changeCL();
        try {
            return canalMsgConsumer.getMessage(timeout, unit);
        } finally {
            revertCL(cl);
        }
    }

    @Override
    public void ack() {
        ClassLoader cl = changeCL();
        try {
            canalMsgConsumer.ack();
        } finally {
            revertCL(cl);
        }
    }

    @Override
    public void rollback() {
        ClassLoader cl = changeCL();
        try {
            canalMsgConsumer.rollback();
        } finally {
            revertCL(cl);
        }
    }

    @Override
    public void disconnect() {
        ClassLoader cl = changeCL();
        try {
            canalMsgConsumer.disconnect();
        } finally {
            revertCL(cl);
        }
    }
}
