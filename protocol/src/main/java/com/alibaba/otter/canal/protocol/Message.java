package com.alibaba.otter.canal.protocol;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.alibaba.otter.canal.common.utils.CanalToStringStyle;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.google.protobuf.ByteString;

/**
 * @author zebin.xuzb @ 2012-6-19
 * @version 1.0.0
 */
public class Message implements Serializable {

    private static final long      serialVersionUID = 1234034768477580009L;
    private long                   id;
    private List<CanalEntry.Entry> entries          = new ArrayList<>();
    // row data for performance, see:
    // https://github.com/alibaba/canal/issues/726
    private boolean                raw              = true;
    private List<ByteString>       rawEntries       = new ArrayList<>();

    public Message(long id, List<Entry> entries){
        this.id = id;
        this.entries = entries == null ? new ArrayList<>() : entries;
        this.raw = false;
    }

    public Message(long id, boolean raw, List entries){
        this.id = id;
        if (raw) {
            this.rawEntries = entries == null ? new ArrayList<>() : entries;
        } else {
            this.entries = entries == null ? new ArrayList<>() : entries;
        }
        this.raw = raw;
    }

    public Message(long id){
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public List<Entry> getEntries() {
        return entries;
    }

    public void setEntries(List<CanalEntry.Entry> entries) {
        this.entries = entries;
    }

    public void addEntry(CanalEntry.Entry entry) {
        this.entries.add(entry);
    }

    public void setRawEntries(List<ByteString> rawEntries) {
        this.rawEntries = rawEntries;
    }

    public void addRawEntry(ByteString rawEntry) {
        this.rawEntries.add(rawEntry);
    }

    public List<ByteString> getRawEntries() {
        return rawEntries;
    }

    public boolean isRaw() {
        return raw;
    }

    public void setRaw(boolean raw) {
        this.raw = raw;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }

}
