package com.alibaba.canal.plumber.stage;

import com.alibaba.canal.plumber.model.EventData;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * process队列对象
 * 异步操作
 * @author dsqin
 * @date 2018/6/5
 */
public class ProcessQueue {

    private ConcurrentLinkedQueue<EventData> queue;

    public ProcessQueue()
    {
        this.queue = new ConcurrentLinkedQueue();
    }

    public boolean offer(EventData eventData)
    {
        return this.queue.offer(eventData);
    }

    public EventData take()
    {
        return this.queue.poll();
    }
}
