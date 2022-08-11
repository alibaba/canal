package com.alibaba.otter.canal.parse.driver.mysql.packets;

import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 类 MariaGTIDSet.java 的实现
 *
 * @author winger 2020/9/24 10:31 上午
 * @version 1.0.0
 */
public class MariaGTIDSet implements GTIDSet {
    //MariaDB 10.0.2+ representation of Gtid
    Map<Long, MariaGtid> gtidMap = new HashMap<>();

    @Override
    public byte[] encode() throws IOException {
        return this.toString().getBytes();
    }

    @Override
    public void update(String str) {
        MariaGtid mariaGtid = MariaGtid.parse(str);
        gtidMap.put(mariaGtid.getDomainId(), mariaGtid);
    }

    public void add(MariaGtid mariaGtid) {
        gtidMap.put(mariaGtid.getDomainId(), mariaGtid);
    }

    public static MariaGTIDSet parse(String gtidData) {
        Map<Long, MariaGtid> gtidMap = new HashMap<>();
        if (StringUtils.isNotEmpty(gtidData)) {
            // 存在多个GTID时会有回车符
            String[] gtidStrs = gtidData.replaceAll("\n", "").split(",");
            for (String gtid : gtidStrs) {
                MariaGtid mariaGtid = MariaGtid.parse(gtid);
                gtidMap.put(mariaGtid.getDomainId(), mariaGtid);
            }
        }
        MariaGTIDSet gtidSet = new MariaGTIDSet();
        gtidSet.gtidMap = gtidMap;
        return gtidSet;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (MariaGtid gtid : gtidMap.values()) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(gtid.toString());
        }
        return sb.toString();
    }
}


