package com.alibaba.otter.canal.parse.inbound.mysql.tsdb.dao;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.orm.ibatis.support.SqlMapClientDaoSupport;

import com.google.common.collect.Maps;

/**
 * canal数据的存储
 *
 * @author wanshao 2017年7月27日 下午10:51:55
 * @since 3.2.5
 */
@SuppressWarnings("deprecation")
public class MetaHistoryDAO extends SqlMapClientDaoSupport {

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
        Connection conn = null;
        InputStream input = null;
        try {
            DataSource dataSource = getDataSource();
            conn = dataSource.getConnection();
            input = Thread.currentThread().getContextClassLoader().getResourceAsStream("ddl/mysql/meta_history.sql");
            if (input == null) {
                return;
            }

            String sql = StringUtils.join(IOUtils.readLines(input), "\n");
            Statement stmt = conn.createStatement();
            stmt.execute(sql);
            stmt.close();
        } catch (Throwable e) {
            logger.warn("init meta_history failed", e);
        } finally {
            IOUtils.closeQuietly(input);
            if (conn != null) {
                conn.close();
            }
        }
    }
}
