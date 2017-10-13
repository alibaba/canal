package com.alibaba.otter.canal.parse.inbound.mysql.tsdb.dao;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

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
public class MetaSnapshotDAO extends SqlMapClientDaoSupport {

    public Long insert(MetaSnapshotDO snapshotDO) {
        return (Long) getSqlMapClientTemplate().insert("meta_snapshot.insert", snapshotDO);
    }

    public Long update(MetaSnapshotDO snapshotDO) {
        return (Long) getSqlMapClientTemplate().insert("meta_snapshot.update", snapshotDO);
    }

    public MetaSnapshotDO findByTimestamp(String destination, Long timestamp) {
        HashMap params = Maps.newHashMapWithExpectedSize(2);
        params.put("timestamp", timestamp == null ? 0L : timestamp);
        params.put("destination", destination);

        return (MetaSnapshotDO) getSqlMapClientTemplate().queryForObject("meta_snapshot.findByTimestamp", params);
    }

    public Integer deleteByName(String destination) {
        HashMap params = Maps.newHashMapWithExpectedSize(2);
        params.put("destination", destination);
        return getSqlMapClientTemplate().delete("meta_snapshot.deleteByName", params);
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
        return getSqlMapClientTemplate().delete("meta_snapshot.deleteByGmtModified", params);
    }

    protected void initDao() throws Exception {
        Connection conn = null;
        InputStream input = null;
        try {
            DataSource dataSource = getDataSource();
            conn = dataSource.getConnection();
            input = Thread.currentThread().getContextClassLoader().getResourceAsStream("ddl/mysql/meta_snapshot.sql");
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
