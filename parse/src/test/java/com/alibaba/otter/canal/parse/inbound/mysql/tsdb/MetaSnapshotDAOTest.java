package com.alibaba.otter.canal.parse.inbound.mysql.tsdb;

import javax.annotation.Resource;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.dao.MetaSnapshotDAO;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.dao.MetaSnapshotDO;

/**
 * Created by wanshao Date: 2017/9/20 Time: 下午5:00
 **/
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "/tsdb/h2-tsdb.xml" })
public class MetaSnapshotDAOTest {

    @Resource
    MetaSnapshotDAO metaSnapshotDAO;

    @Ignore
    @Test
    public void testSimple() {
        MetaSnapshotDO metaSnapshotDO = new MetaSnapshotDO();
        metaSnapshotDO.setDestination("test");
        metaSnapshotDO.setBinlogFile("000001");
        metaSnapshotDO.setBinlogOffest(4L);
        metaSnapshotDO.setBinlogMasterId("1");
        metaSnapshotDO.setBinlogTimestamp(System.currentTimeMillis() - 7300 * 1000);
        metaSnapshotDO.setData("test");
        metaSnapshotDAO.insert(metaSnapshotDO);

        MetaSnapshotDO snapshotDO = metaSnapshotDAO.findByTimestamp("test", System.currentTimeMillis());
        System.out.println(snapshotDO);

        int count = metaSnapshotDAO.deleteByTimestamp("test", 7200);
        System.out.println(count);
    }

}
