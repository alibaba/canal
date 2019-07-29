package com.alibaba.otter.canal.sink.stub;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.store.CanalEventStore;
import com.alibaba.otter.canal.store.CanalStoreException;
import com.alibaba.otter.canal.store.model.Event;
import com.alibaba.otter.canal.store.model.Events;

public class DummyEventStore implements CanalEventStore<Event> {

    public void ack(Position position) throws CanalStoreException {

    }

    public void ack(Position position, Long seqId) throws CanalStoreException {

    }

    public Events get(Position start, int batchSize) throws InterruptedException, CanalStoreException {
        return null;
    }

    public Events get(Position start, int batchSize, long timeout, TimeUnit unit) throws InterruptedException,
                                                                                 CanalStoreException {
        return null;
    }

    public Position getFirstPosition() throws CanalStoreException {
        return null;
    }

    public Position getLatestPosition() throws CanalStoreException {
        return null;
    }

    public void rollback() throws CanalStoreException {

    }

    public Events tryGet(Position start, int batchSize) throws CanalStoreException {
        return null;
    }

    public boolean isStart() {
        return false;
    }

    public void start() {

    }

    public void stop() {

    }

    public void cleanAll() throws CanalStoreException {
    }

    public void cleanUntil(Position position) throws CanalStoreException {

    }

    public void put(Event data) throws InterruptedException, CanalStoreException {
        System.out.println("time:" + data.getExecuteTime());
    }

    public boolean put(Event data, long timeout, TimeUnit unit) throws InterruptedException, CanalStoreException {
        System.out.println("time:" + data.getExecuteTime());
        return true;
    }

    public boolean tryPut(Event data) throws CanalStoreException {
        System.out.println("time:" + data.getExecuteTime());
        return true;
    }

    public void put(List<Event> datas) throws InterruptedException, CanalStoreException {
        Event data = datas.get(0);
        System.out.println("time:" + data.getExecuteTime());
    }

    public boolean put(List<Event> datas, long timeout, TimeUnit unit) throws InterruptedException, CanalStoreException {
        Event data = datas.get(0);
        System.out.println("time:" + data.getExecuteTime());
        return true;
    }

    public boolean tryPut(List<Event> datas) throws CanalStoreException {
        Event data = datas.get(0);
        System.out.println("time:" + data.getExecuteTime());
        return true;
    }

}
