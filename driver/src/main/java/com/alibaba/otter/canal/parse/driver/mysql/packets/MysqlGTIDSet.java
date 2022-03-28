package com.alibaba.otter.canal.parse.driver.mysql.packets;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.otter.canal.parse.driver.mysql.utils.ByteHelper;

/**
 * Created by hiwjd on 2018/4/23. hiwjd0@gmail.com
 */
public class MysqlGTIDSet implements GTIDSet {

    public Map<String, UUIDSet> sets;

    @Override
    public byte[] encode() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteHelper.writeUnsignedInt64LittleEndian(sets.size(), out);

        for (Map.Entry<String, UUIDSet> entry : sets.entrySet()) {
            out.write(entry.getValue().encode());
        }

        return out.toByteArray();
    }

    @Override
    public void update(String str) {
        UUIDSet us = UUIDSet.parse(str);
        String sid = us.SID.toString();
        if (sets.containsKey(sid)) {
            sets.get(sid).intervals.addAll(us.intervals);
            sets.get(sid).intervals = UUIDSet.combine(sets.get(sid).intervals);
        } else {
            sets.put(sid, us);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) return false;
        if (this == o) return true;

        MysqlGTIDSet gs = (MysqlGTIDSet) o;
        if (gs.sets == null) return false;

        for (Map.Entry<String, UUIDSet> entry : sets.entrySet()) {
            if (!entry.getValue().equals(gs.sets.get(entry.getKey()))) {
                return false;
            }
        }

        return true;
    }

    /**
     * 解析如下格式的字符串为MysqlGTIDSet: 726757ad-4455-11e8-ae04-0242ac110002:1 =>
     * MysqlGTIDSet{ sets: { 726757ad-4455-11e8-ae04-0242ac110002: UUIDSet{ SID:
     * 726757ad-4455-11e8-ae04-0242ac110002, intervals: [{start:1, stop:2}] } }
     * } 726757ad-4455-11e8-ae04-0242ac110002:1-3 => MysqlGTIDSet{ sets: {
     * 726757ad-4455-11e8-ae04-0242ac110002: UUIDSet{ SID:
     * 726757ad-4455-11e8-ae04-0242ac110002, intervals: [{start:1, stop:4}] } }
     * } 726757ad-4455-11e8-ae04-0242ac110002:1-3:4 => MysqlGTIDSet{ sets: {
     * 726757ad-4455-11e8-ae04-0242ac110002: UUIDSet{ SID:
     * 726757ad-4455-11e8-ae04-0242ac110002, intervals: [{start:1, stop:5}] } }
     * } 726757ad-4455-11e8-ae04-0242ac110002:1-3:7-9 => MysqlGTIDSet{ sets: {
     * 726757ad-4455-11e8-ae04-0242ac110002: UUIDSet{ SID:
     * 726757ad-4455-11e8-ae04-0242ac110002, intervals: [{start:1, stop:4},
     * {start:7, stop: 10}] } } }
     * 726757ad-4455-11e8-ae04-0242ac110002:1-3,726757
     * ad-4455-11e8-ae04-0242ac110003:4 => MysqlGTIDSet{ sets: {
     * 726757ad-4455-11e8-ae04-0242ac110002: UUIDSet{ SID:
     * 726757ad-4455-11e8-ae04-0242ac110002, intervals: [{start:1, stop:4}] },
     * 726757ad-4455-11e8-ae04-0242ac110003: UUIDSet{ SID:
     * 726757ad-4455-11e8-ae04-0242ac110002, intervals: [{start:4, stop:5}] } }
     * }
     *
     * @param gtidData
     * @return
     */
    public static MysqlGTIDSet parse(String gtidData) {
        Map<String, UUIDSet> m;

        if (gtidData == null || gtidData.length() < 1) {
            m = new HashMap<>();
        } else {
            // 存在多个GTID时会有回车符
            String[] uuidStrs = gtidData.replaceAll("\n", "").split(",");
            m = new HashMap<>(uuidStrs.length);
            for (String uuidStr : uuidStrs) {
                UUIDSet uuidSet = UUIDSet.parse(uuidStr);
                m.put(uuidSet.SID.toString(), uuidSet);
            }
        }

        MysqlGTIDSet gs = new MysqlGTIDSet();
        gs.sets = m;

        return gs;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<String, UUIDSet> entry : sets.entrySet()) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(entry.getValue().toString());
        }

        return sb.toString();
    }
}
