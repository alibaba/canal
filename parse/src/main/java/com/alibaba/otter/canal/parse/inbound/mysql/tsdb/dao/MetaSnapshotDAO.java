package com.alibaba.otter.canal.parse.inbound.mysql.tsdb.dao;

import java.util.HashMap;

import com.google.common.collect.Maps;

/**
 * canal数据的存储
 * 
 * @author wanshao 2017年7月27日 下午10:51:55
 * @since 3.2.5
 */
public class MetaSnapshotDAO extends MetaBaseDAO {

    public Integer insert(MetaSnapshotDO snapshotDO) {
        return getSqlSessionTemplate().insert("meta_snapshot.insert", snapshotDO);
    }

    public Integer update(MetaSnapshotDO snapshotDO) {
        return getSqlSessionTemplate().insert("meta_snapshot.update", snapshotDO);
    }

    public MetaSnapshotDO findByTimestamp(String destination, Long timestamp) {
        HashMap params = Maps.newHashMapWithExpectedSize(2);
        params.put("timestamp", timestamp == null ? 0L : timestamp);
        params.put("destination", destination);

        return (MetaSnapshotDO) getSqlSessionTemplate().selectOne("meta_snapshot.findByTimestamp", params);
    }

    public Integer deleteByName(String destination) {
        HashMap params = Maps.newHashMapWithExpectedSize(2);
        params.put("destination", destination);
        return getSqlSessionTemplate().delete("meta_snapshot.deleteByName", params);
    }

    /**
     * 删除interval秒之前的数据
     */
    public Integer deleteByTimestamp(String destination, int interval) {
        HashMap params = Maps.newHashMapWithExpectedSize(2);
        long timestamp = System.currentTimeMillis() - interval * 1000;
        params.put("timestamp", timestamp);
        params.put("destination", destination);
        return getSqlSessionTemplate().delete("meta_snapshot.deleteByTimestamp", params);
    }

    protected void initDao() throws Exception {
        initTable("meta_snapshot");
    }

}
