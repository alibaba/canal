package com.alibaba.canal.plumber.model;

import com.alibaba.otter.canal.protocol.CanalEntry;
import java.io.Serializable;
import java.util.Map;

/**
 * event数据对象
 * @author dsqin
 * @date 2018/6/5
 */
public class EventData implements Serializable
{
    /**
     * header头
     */
    private Map<String, String> headers;

    /**
     * body内容
     */
    private String body;

    /**
     * 事件类型
     */
    private EventType eventType;

    /**
     * 使用的schema对象
     */
    private String schemaName;

    /**
     * 数据对象唯一编号
     */
    private long sequenceId;

    /**
     * 处理编号
     */
    private int stage;

    public EventData() {}

    public EventData(CanalEntry.Entry entry)
    {
        this.body = entry.getStoreValue().toStringUtf8();
        CanalEntry.Header header = entry.getHeader();
        this.eventType = EventType.valuesOf(header.getEventType().getNumber());
        this.schemaName = entry.getHeader().getSchemaName();
        this.sequenceId = entry.getHeader().getServerId();
        this.stage = 1;
    }

    public Map<String, String> getHeaders()
    {
        return this.headers;
    }

    public void setHeaders(Map<String, String> headers)
    {
        this.headers = headers;
    }

    public String getBody()
    {
        return this.body;
    }

    public String getBodyStringUtf8()
    {
        return String.valueOf(this.body);
    }

    public void setBody(String body)
    {
        this.body = body;
    }

    public EventType getEventType()
    {
        return this.eventType;
    }

    public void setEventType(EventType eventType)
    {
        this.eventType = eventType;
    }

    public String getSchemaName()
    {
        return this.schemaName;
    }

    public void setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
    }

    public long getSequenceId()
    {
        return this.sequenceId;
    }

    public void setSequenceId(long sequenceId)
    {
        this.sequenceId = sequenceId;
    }

    public int getStage()
    {
        return this.stage;
    }

    public void incrStage()
    {
        this.stage += 1;
    }

    public void setStage(int stage)
    {
        this.stage = stage;
    }
}
