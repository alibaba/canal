package com.alibaba.otter.canal.parse.inbound.mysql.tsdb.dao;

import java.util.HashMap;

import org.springframework.orm.ibatis.support.SqlMapClientDaoSupport;

import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.model.MetaSnapshotDO;
import com.google.common.collect.Maps;

/**
 * canal数据的存储
 * 
 * @author wanshao 2017年7月27日 下午10:51:55
 * @since 3.2.5
 */

public class MetaSnapshotDAO extends SqlMapClientDaoSupport {

    public Long insert(MetaSnapshotDO snapshotDO) {
        return (Long) getSqlMapClientTemplate().insert("table_meta_snapshot.insert", snapshotDO);
    }

    public Long update(MetaSnapshotDO snapshotDO) {
        return (Long) getSqlMapClientTemplate().insert("table_meta_snapshot.update", snapshotDO);
    }

    public MetaSnapshotDO findByTimestamp(long timestamp) {
        HashMap params = Maps.newHashMapWithExpectedSize(2);
        params.put("timestamp", timestamp);
        return (MetaSnapshotDO) getSqlMapClientTemplate().queryForObject("table_meta_snapshot.findByTimestamp", params);
    }

    public Integer deleteByTask(String taskName) {
        HashMap params = Maps.newHashMapWithExpectedSize(2);
        params.put("taskName", taskName);
        return getSqlMapClientTemplate().delete("table_meta_snapshot.deleteByTaskName", params);
    }

    /**
     * 删除interval秒之前的数据
     */
    public Integer deleteByGmtModified(int interval) {
        HashMap params = Maps.newHashMapWithExpectedSize(2);
        params.put("interval", interval);
        return getSqlMapClientTemplate().delete("table_meta_snapshot.deleteByGmtModified", params);
    }

}
