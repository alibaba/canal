package com.alibaba.otter.canal.parse.inbound.mysql.tsdb.dao;

import java.text.SimpleDateFormat;
import java.util.Date;
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
        return (Long) getSqlMapClientTemplate().insert("meta_history.insert", metaDO);
    }

    public List<MetaHistoryDO> findByTimestamp(String destination, Long snapshotTimestamp, Long timestamp) {
        HashMap params = Maps.newHashMapWithExpectedSize(2);
        params.put("destination", destination);
        params.put("snapshotTimestamp", snapshotTimestamp == null ? 0L : snapshotTimestamp);
        params.put("timestamp", timestamp == null ? 0L : timestamp);
        return (List<MetaHistoryDO>) getSqlMapClientTemplate().queryForList("meta_history.findByTimestamp", params);
    }

    public Integer deleteByName(String destination) {
        HashMap params = Maps.newHashMapWithExpectedSize(2);
        params.put("destination", destination);
        return getSqlMapClientTemplate().delete("meta_history.deleteByName", params);
    }

    /**
     * 删除interval秒之前的数据
     */
    public Integer deleteByGmtModified(int interval) {
        HashMap params = Maps.newHashMapWithExpectedSize(2);
        long timestamp = System.currentTimeMillis() - interval * 1000;
        Date date = new Date(timestamp);
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        params.put("timestamp", format.format(date));
        return getSqlMapClientTemplate().delete("meta_history.deleteByGmtModified", params);
    }

    protected void initDao() throws Exception {
        initTable("meta_history");
    }
}
