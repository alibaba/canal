package com.alibaba.canal.plumber.model;

/**
 * 事件类型
 * @author dsqin
 * @date 2018/6/5
 */
public enum EventType {
    INSERT(1),  UPDATE(2),  DELETE(3);

    private int value;

    private EventType(int value)
    {
        this.value = value;
    }

    public boolean isInsert()
    {
        return equals(INSERT);
    }

    public boolean isUpdate()
    {
        return equals(UPDATE);
    }

    public boolean isDelete()
    {
        return equals(DELETE);
    }

    public static EventType valuesOf(int value)
    {
        EventType[] eventTypes = values();
        for (EventType eventType : eventTypes) {
            if (eventType.value == value) {
                return eventType;
            }
        }
        return null;
    }

    public int getValue()
    {
        return this.value;
    }

    public void setValue(int value)
    {
        this.value = value;
    }
}
