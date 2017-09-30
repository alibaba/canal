package com.alibaba.otter.canal.parse.inbound.mysql.tsdb.dao;

import java.util.HashMap;
import java.util.List;

import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.model.MetaHistoryDO;

import com.google.common.collect.Maps;
import org.springframework.orm.ibatis.support.SqlMapClientDaoSupport;

/**
 * canal数据的存储
 *
 * @author wanshao 2017年7月27日 下午10:51:55
 * @since 3.2.5
 */
@SuppressWarnings("deprecation")
public class MetaHistoryDAO extends SqlMapClientDaoSupport {

    public List<MetaHistoryDO> getAll() {
        return getSqlMapClientTemplate().queryForList("table_meta_history.getAll");
    }

    public Long insert(MetaHistoryDO metaDO) {
        return (Long)getSqlMapClientTemplate().insert("table_meta_history.insert", metaDO);
    }

    public List<MetaHistoryDO> findByTimestamp(long snapshotTimestamp, long timestamp) {
        HashMap params = Maps.newHashMapWithExpectedSize(2);
        params.put("snapshotTimestamp", snapshotTimestamp);
        params.put("timestamp", timestamp);
        return (List<MetaHistoryDO>)getSqlMapClientTemplate().queryForList("table_meta_history.findByTimestamp",
            params);
    }

    /**
     * 删除interval秒之前的数据
     */
    public Integer deleteByGmtModified(int interval) {
        HashMap params = Maps.newHashMapWithExpectedSize(2);
        params.put("interval", interval);
        return getSqlMapClientTemplate().delete("table_meta_history.deleteByGmtModified", params);
    }
}
