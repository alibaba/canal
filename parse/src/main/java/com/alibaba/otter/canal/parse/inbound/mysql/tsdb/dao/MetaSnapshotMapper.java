package com.alibaba.otter.canal.parse.inbound.mysql.tsdb.dao;

import java.util.Map;

public interface MetaSnapshotMapper {

    MetaSnapshotDO findByTimestamp(Map<String, Object> map);

    long insert(MetaSnapshotDO metaSnapshotDO);

    long update(MetaSnapshotDO metaSnapshotDO);

    int deleteByName(Map<String, Object> map);

    int deleteByTimestamp(Map<String, Object> map);
}
