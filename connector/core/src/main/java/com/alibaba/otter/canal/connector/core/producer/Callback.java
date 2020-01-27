package com.alibaba.otter.canal.connector.core.producer;

/**
 * MQ 回调类
 *
 * @author rewerma 2020-01-27
 * @version 1.0.0
 */
public interface Callback {
    void commit();

    void rollback();
}
