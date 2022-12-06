package com.alibaba.otter.canal.parse.driver.mysql;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import com.alibaba.otter.canal.parse.driver.mysql.packets.GTIDSet;
import com.alibaba.otter.canal.parse.driver.mysql.packets.MysqlGTIDSet;
import com.alibaba.otter.canal.parse.driver.mysql.packets.UUIDSet;

/**
 * Created by hiwjd on 2018/4/25. hiwjd0@gmail.com
 */
public class MysqlGTIDSetTest {

    @Test
    public void testEncode() throws IOException {
        GTIDSet gtidSet = MysqlGTIDSet.parse("726757ad-4455-11e8-ae04-0242ac110002:1");
        byte[] bytes = gtidSet.encode();

        byte[] expected = { 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x72, 0x67, 0x57, (byte) 0xad, 0x44, 0x55,
                0x11, (byte) 0xe8, (byte) 0xae, 0x04, 0x02, 0x42, (byte) 0xac, 0x11, 0x00, 0x02, 0x01, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00 };

        for (int i = 0; i < bytes.length; i++) {
            assertEquals(expected[i], bytes[i]);
        }
    }

    @Test
    public void testUpdate() {
        String gtid1 = "726757ad-4455-11e8-ae04-0242ac110002:1-25536412";
        MysqlGTIDSet mysqlGTIDSet1 = MysqlGTIDSet.parse(gtid1);

        String gtid2 = "726757ad-4455-11e8-ae04-0242ac110002:1-20304074";
        MysqlGTIDSet mysqlGTIDSet2 = MysqlGTIDSet.parse(gtid2);

        mysqlGTIDSet1.update(gtid2);
        assertEquals("726757ad-4455-11e8-ae04-0242ac110002:1-25536412", mysqlGTIDSet1.toString());
    }

    @Test
    public void testParse() {
        Map<String, MysqlGTIDSet> cases = new HashMap<>(5);
        cases.put("726757ad-4455-11e8-ae04-0242ac110002:1",
            buildForTest(new Material("726757ad-4455-11e8-ae04-0242ac110002", 1, 2)));
        cases.put("726757ad-4455-11e8-ae04-0242ac110002:1-3",
            buildForTest(new Material("726757ad-4455-11e8-ae04-0242ac110002", 1, 4)));
        cases.put("726757ad-4455-11e8-ae04-0242ac110002:1-3:4",
            buildForTest(new Material("726757ad-4455-11e8-ae04-0242ac110002", 1, 5)));
        cases.put("726757ad-4455-11e8-ae04-0242ac110002:1-3:7-9",
            buildForTest(new Material("726757ad-4455-11e8-ae04-0242ac110002", 1, 4, 7, 10)));
        cases.put("726757ad-4455-11e8-ae04-0242ac110002:1-3,726757ad-4455-11e8-ae04-0242ac110003:4",
            buildForTest(Arrays.asList(new Material("726757ad-4455-11e8-ae04-0242ac110002", 1, 4),
                new Material("726757ad-4455-11e8-ae04-0242ac110003", 4, 5))));

        for (Map.Entry<String, MysqlGTIDSet> entry : cases.entrySet()) {
            MysqlGTIDSet expected = entry.getValue();
            MysqlGTIDSet actual = MysqlGTIDSet.parse(entry.getKey());

            assertEquals(expected, actual);
        }
    }

    private static class Material {

        public Material(String uuid, long start, long stop){
            this.uuid = uuid;
            this.start = start;
            this.stop = stop;
            this.start1 = 0;
            this.stop1 = 0;
        }

        public Material(String uuid, long start, long stop, long start1, long stop1){
            this.uuid = uuid;
            this.start = start;
            this.stop = stop;
            this.start1 = start1;
            this.stop1 = stop1;
        }

        public String uuid;
        public long   start;
        public long   stop;
        public long   start1;
        public long   stop1;
    }

    private MysqlGTIDSet buildForTest(Material material) {
        return buildForTest(Arrays.asList(material));
    }

    private MysqlGTIDSet buildForTest(List<Material> materials) {
        Map<String, UUIDSet> sets = new HashMap<>();
        for (Material a : materials) {
            UUIDSet.Interval interval = new UUIDSet.Interval();
            interval.start = a.start;
            interval.stop = a.stop;
            List<UUIDSet.Interval> intervals = new ArrayList<>();
            intervals.add(interval);

            if (a.start1 > 0 && a.stop1 > 0) {
                UUIDSet.Interval interval1 = new UUIDSet.Interval();
                interval1.start = a.start1;
                interval1.stop = a.stop1;
                intervals.add(interval1);
            }

            UUIDSet us = new UUIDSet();
            us.SID = UUID.fromString(a.uuid);
            us.intervals = intervals;

            sets.put(a.uuid, us);
        }

        MysqlGTIDSet gs = new MysqlGTIDSet();
        gs.sets = sets;

        return gs;
    }
}
