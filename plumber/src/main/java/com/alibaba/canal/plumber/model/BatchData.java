package com.alibaba.canal.plumber.model;

import java.io.Serializable;
import java.util.List;

/**
 * 批量写入数据对象
 * @author dsqin
 * @date 2018/6/5
 */
public class BatchData  implements Serializable
{
    private List<EventData> eventBatch;

    public BatchData(List<EventData> eventBatch)
    {
        this.eventBatch = eventBatch;
    }

    public List<EventData> getEventBatch()
    {
        return this.eventBatch;
    }

    public void setEventBatch(List<EventData> eventBatch)
    {
        this.eventBatch = eventBatch;
    }
}

