package com.alibaba.otter.canal.connector.core.producer;

public interface Callback {
    void commit();

    void rollback();
}
