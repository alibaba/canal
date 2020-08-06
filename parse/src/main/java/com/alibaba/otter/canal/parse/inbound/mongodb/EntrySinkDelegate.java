package com.alibaba.otter.canal.parse.inbound.mongodb;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.List;

public interface EntrySinkDelegate {
    void sink(List<CanalEntry.Entry> entries) throws InterruptedException;
}
