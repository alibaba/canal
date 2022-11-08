package com.alibaba.otter.canal.parse.inbound.mysql.tsdb.dao;

import java.util.HashMap;
import java.util.List;

import com.google.common.collect.Maps;

/**
 * canal数据的存储
 *
 * @author wanshao 2017年7月27日 下午10:51:55
 * @since 3.2.5
 */
@SuppressWarnings("deprecation")
public class MetaHistoryDAO extends MetaBaseDAO {

    public Long insert(MetaHistoryDO metaDO) {
        return getSqlSession().getMapper(MetaHistoryMapper.class).insert(metaDO);
    }

    public List<MetaHistoryDO> findByTimestamp(String destination, Long snapshotTimestamp, Long timestamp) {
        HashMap params = Maps.newHashMapWithExpectedSize(2);
        params.put("destination", destination);
        params.put("snapshotTimestamp", snapshotTimestamp == null ? 0L : snapshotTimestamp);
        params.put("timestamp", timestamp == null ? 0L : timestamp);
        return getSqlSession().getMapper(MetaHistoryMapper.class).findByTimestamp(params);
    }

    public Integer deleteByName(String destination) {
        HashMap params = Maps.newHashMapWithExpectedSize(2);
        params.put("destination", destination);
        return getSqlSession().getMapper(MetaHistoryMapper.class).deleteByName(params);
    }

    /**
     * 删除interval秒之前的数据
     */
    public Integer deleteByTimestamp(String destination, int interval) {
        HashMap params = Maps.newHashMapWithExpectedSize(2);
        long timestamp = System.currentTimeMillis() - interval * 1000;
        params.put("timestamp", timestamp);
        params.put("destination", destination);
        return getSqlSession().getMapper(MetaHistoryMapper.class).deleteByTimestamp(params);
    }

    protected void initDao() throws Exception {
        initTable("meta_history");
    }
}
