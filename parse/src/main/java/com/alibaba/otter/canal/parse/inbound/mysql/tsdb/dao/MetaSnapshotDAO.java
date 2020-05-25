package com.alibaba.otter.canal.parse.inbound.mysql.tsdb.dao;

import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.mapper.MetaSnapshotMapper;
import com.google.common.collect.Maps;

import java.util.HashMap;

/**
 * canal数据的存储
 *
 * @author wanshao 2017年7月27日 下午10:51:55
 * @since 3.2.5
 */
public class MetaSnapshotDAO extends MetaBaseDAO {

    public Integer insert(MetaSnapshotDO snapshotDO) {
        return getSqlSession().getMapper(MetaSnapshotMapper.class).insert(snapshotDO);
    }

    public Integer update(MetaSnapshotDO snapshotDO) {
        return getSqlSession().getMapper(MetaSnapshotMapper.class).update(snapshotDO);
    }

    public MetaSnapshotDO findByTimestamp(String destination, Long timestamp) {
        HashMap<String, Object> params = Maps.newHashMapWithExpectedSize(2);
        params.put("timestamp", timestamp == null ? 0L : timestamp);
        params.put("destination", destination);
        return getSqlSession().getMapper(MetaSnapshotMapper.class).findByTimestamp(params);
    }

    public Integer deleteByName(String destination) {
        HashMap<String, Object> params = Maps.newHashMapWithExpectedSize(2);
        params.put("destination", destination);
        return getSqlSession().getMapper(MetaSnapshotMapper.class).deleteByName(params);
    }

    /**
     * 删除interval秒之前的数据
     */
    public Integer deleteByTimestamp(String destination, int interval) {
        HashMap<String, Object> params    = Maps.newHashMapWithExpectedSize(2);
        long                    timestamp = System.currentTimeMillis() - interval * 1000;
        params.put("timestamp", timestamp);
        params.put("destination", destination);
        return getSqlSession().getMapper(MetaSnapshotMapper.class).deleteByTimestamp(params);
    }

    @Override
    protected void initDao() throws Exception {
        initTable("meta_snapshot");
    }

}
