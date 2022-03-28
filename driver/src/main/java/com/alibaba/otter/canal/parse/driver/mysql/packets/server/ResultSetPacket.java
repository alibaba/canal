package com.alibaba.otter.canal.parse.driver.mysql.packets.server;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

public class ResultSetPacket {

    private SocketAddress     sourceAddress;
    private List<FieldPacket> fieldDescriptors = new ArrayList<>();
    private List<String>      fieldValues      = new ArrayList<>();

    public void setFieldDescriptors(List<FieldPacket> fieldDescriptors) {
        this.fieldDescriptors = fieldDescriptors;
    }

    public List<FieldPacket> getFieldDescriptors() {
        return fieldDescriptors;
    }

    public void setFieldValues(List<String> fieldValues) {
        this.fieldValues = fieldValues;
    }

    public List<String> getFieldValues() {
        return fieldValues;
    }

    public void setSourceAddress(SocketAddress sourceAddress) {
        this.sourceAddress = sourceAddress;
    }

    public SocketAddress getSourceAddress() {
        return sourceAddress;
    }

    public String toString() {
        return "ResultSetPacket [fieldDescriptors=" + fieldDescriptors + ", fieldValues=" + fieldValues
               + ", sourceAddress=" + sourceAddress + "]";
    }

}
