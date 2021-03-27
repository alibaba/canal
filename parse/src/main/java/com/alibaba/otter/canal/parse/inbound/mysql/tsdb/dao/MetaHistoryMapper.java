package com.alibaba.otter.canal.parse.inbound.mysql.tsdb.dao;

import java.util.List;
import java.util.Map;

public interface MetaHistoryMapper {

    List<MetaHistoryDO> findByTimestamp(Map<String, Object> map);

    long insert(MetaHistoryDO metaHistoryDO);

    int deleteByName(Map<String, Object> map);

    int deleteByTimestamp(Map<String, Object> map);
}
