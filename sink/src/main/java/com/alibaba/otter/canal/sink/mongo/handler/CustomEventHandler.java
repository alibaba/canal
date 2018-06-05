package com.alibaba.otter.canal.sink.mongo.handler;

import com.alibaba.otter.canal.sink.AbstractCanalEventDownStreamHandler;
import com.alibaba.otter.canal.store.model.Event;

import java.util.List;

/**
 * 自定义事件处理器
 * @author dsqin
 * @date 2018/5/17
 */
public class CustomEventHandler extends AbstractCanalEventDownStreamHandler<List<Event>> {

    @Override
    public List<Event> before(List<Event> events) {
        return super.before(events);
    }

    @Override
    public List<Event> retry(List<Event> events) {
        return super.retry(events);
    }

    @Override
    public List<Event> after(List<Event> events) {
        return super.after(events);
    }
}
