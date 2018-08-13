package com.alibaba.otter.canal.server;

import java.util.concurrent.TimeUnit;

import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.server.exception.CanalServerException;
import com.alibaba.otter.canal.store.CanalEventTooLargeException;

public interface CanalService {

    void subscribe(ClientIdentity clientIdentity) throws CanalServerException;

    void unsubscribe(ClientIdentity clientIdentity) throws CanalServerException;

    Message get(ClientIdentity clientIdentity, int batchSize) throws CanalServerException;

    Message get(ClientIdentity clientIdentity, int batchSize, Long timeout, TimeUnit unit) throws CanalServerException;

    Message getWithoutAck(ClientIdentity clientIdentity, int batchSize) throws CanalServerException;

    Message getWithoutAck(ClientIdentity clientIdentity, int batchSize, Long timeout, TimeUnit unit)
                                                                                                    throws CanalServerException;
    Message get(ClientIdentity clientIdentity, int batchSize, long batchTransactionMaxSize) throws CanalServerException, CanalEventTooLargeException;

    Message get(ClientIdentity clientIdentity, int batchSize, Long timeout, TimeUnit unit, long batchTransactionMaxSize) throws CanalServerException, CanalEventTooLargeException;

    Message getWithoutAck(ClientIdentity clientIdentity, int batchSize, long batchTransactionMaxSize) throws CanalServerException, CanalEventTooLargeException;

    Message getWithoutAck(ClientIdentity clientIdentity, int batchSize, Long timeout, TimeUnit unit, long batchTransactionMaxSize)
            throws CanalServerException, CanalEventTooLargeException;

    Message getFirstEvent(ClientIdentity clientIdentity, int batchSize, Long timeout, TimeUnit unit) throws InterruptedException;

    void ack(ClientIdentity clientIdentity, long batchId) throws CanalServerException;

    void rollback(ClientIdentity clientIdentity) throws CanalServerException;

    void rollback(ClientIdentity clientIdentity, Long batchId) throws CanalServerException;
}
