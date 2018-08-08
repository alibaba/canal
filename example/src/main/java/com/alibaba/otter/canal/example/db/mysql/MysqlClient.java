package com.alibaba.otter.canal.example.db.mysql;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.List;

public class MysqlClient extends AbstractMysqlClient {

    @Override
    public void insert(CanalEntry.Header header, List<CanalEntry.Column> afterColumns) {
        execute(header, afterColumns);
    }

    @Override
    public void update(CanalEntry.Header header, List<CanalEntry.Column> afterColumns) {
        execute(header, afterColumns);
    }

    @Override
    public void delete(CanalEntry.Header header, List<CanalEntry.Column> beforeColumns) {
        execute(header, beforeColumns);
    }
}
