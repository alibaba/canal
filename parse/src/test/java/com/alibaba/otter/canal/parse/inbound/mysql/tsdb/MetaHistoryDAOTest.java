package com.alibaba.otter.canal.parse.inbound.mysql.tsdb;

import java.util.List;

import javax.annotation.Resource;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.dao.MetaHistoryDAO;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.dao.MetaHistoryDO;

/**
 * Created by wanshao Date: 2017/9/20 Time: 下午5:00
 **/
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "/tsdb/h2-tsdb.xml" })
@Ignore
public class MetaHistoryDAOTest {

    @Resource
    MetaHistoryDAO metaHistoryDAO;

    @Test
    public void testSimple() {
        MetaHistoryDO historyDO = new MetaHistoryDO();
        historyDO.setDestination("test");
        historyDO.setBinlogFile("000001");
        historyDO.setBinlogOffest(4L);
        historyDO.setBinlogMasterId("1");
        historyDO.setBinlogTimestamp(System.currentTimeMillis() - 7300 * 1000);
        historyDO.setSqlSchema("test");
        historyDO.setUseSchema("test");
        historyDO.setSqlTable("testTable");
        historyDO.setSqlTable("drop table testTable");
        metaHistoryDAO.insert(historyDO);

        int count = metaHistoryDAO.deleteByTimestamp("test", 7200);
        System.out.println(count);

        List<MetaHistoryDO> metaHistoryDOList = metaHistoryDAO.findByTimestamp("test", 0L, System.currentTimeMillis());
        for (MetaHistoryDO metaHistoryDO : metaHistoryDOList) {
            System.out.println(metaHistoryDO.getId());
        }
    }

}
