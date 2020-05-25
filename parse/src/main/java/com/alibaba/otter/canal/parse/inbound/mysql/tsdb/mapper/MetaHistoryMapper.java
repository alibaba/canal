package com.alibaba.otter.canal.parse.inbound.mysql.tsdb.mapper;

import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.dao.MetaHistoryDO;

import java.util.List;
import java.util.Map;

/**
 * MetaHistoryMapper
 *
 * @author yakir on 2020/05/25 11:15.
 */
public interface MetaHistoryMapper {

    List<MetaHistoryDO> findByTimestamp(Map<String, Object> map);

    int insert(MetaHistoryDO metaHistoryDO);

    int deleteByName(Map<String, Object> map);

    int deleteByTimestamp(Map<String, Object> map);

}
