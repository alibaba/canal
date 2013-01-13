package com.alibaba.otter.canal.parse.inbound.mysql.networking.packets.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.alibaba.otter.canal.parse.inbound.mysql.networking.packets.CommandPacket;

public class QueryCommandPacket extends CommandPacket {

    private String queryString;

    public QueryCommandPacket(){
        setCommand((byte) 0x03);
    }

    public void fromBytes(byte[] data) throws IOException {
    }

    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(getCommand());
        out.write(getQueryString().getBytes());
        return out.toByteArray();
    }

    public void setQueryString(String queryString) {
        this.queryString = queryString;
    }

    public String getQueryString() {
        return queryString;
    }

}
