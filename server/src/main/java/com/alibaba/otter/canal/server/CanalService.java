package com.alibaba.otter.canal.server;

import java.util.concurrent.TimeUnit;

import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.server.exception.CanalServerException;

public interface CanalService {

    void subscribe(ClientIdentity clientIdentity) throws CanalServerException;

    void unsubscribe(ClientIdentity clientIdentity) throws CanalServerException;

    Message get(ClientIdentity clientIdentity, int batchSize) throws CanalServerException;

    Message get(ClientIdentity clientIdentity, int batchSize, Long timeout, TimeUnit unit) throws CanalServerException;

    Message getWithoutAck(ClientIdentity clientIdentity, int batchSize) throws CanalServerException;

    Message getWithoutAck(ClientIdentity clientIdentity, int batchSize, Long timeout, TimeUnit unit)
                                                                                                    throws CanalServerException;

    void ack(ClientIdentity clientIdentity, long batchId) throws CanalServerException;

    void rollback(ClientIdentity clientIdentity) throws CanalServerException;

    void rollback(ClientIdentity clientIdentity, Long batchId) throws CanalServerException;
}
