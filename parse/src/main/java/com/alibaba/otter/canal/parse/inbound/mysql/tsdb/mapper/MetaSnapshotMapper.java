package com.alibaba.otter.canal.parse.inbound.mysql.tsdb.mapper;

import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.dao.MetaSnapshotDO;

import java.util.Map;

/**
 * MetaSnapshotMapper
 *
 * @author yakir on 2020/05/25 11:15.
 */
public interface MetaSnapshotMapper {

    MetaSnapshotDO findByTimestamp(Map<String, Object> map);

    int insert(MetaSnapshotDO metaSnapshotDO);

    int update(MetaSnapshotDO metaSnapshotDO);

    int deleteByName(Map<String, Object> map);

    int deleteByTimestamp(Map<String, Object> map);

}
