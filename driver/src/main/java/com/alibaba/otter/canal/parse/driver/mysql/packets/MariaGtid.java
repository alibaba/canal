package com.alibaba.otter.canal.parse.driver.mysql.packets;

import org.apache.commons.lang.math.NumberUtils;

/**
 * 类 MariaGtid.java 的实现
 *
 * @author winger 2020/9/24 11:30 上午
 * @version 1.0.0
 */
public class MariaGtid {

    // {domainId}-{serverId}-{sequence}
    private long domainId;
    private long serverId;
    private long sequence;

    public MariaGtid(long domainId, long serverId, long sequence) {
        this.domainId = domainId;
        this.serverId = serverId;
        this.sequence = sequence;
    }

    public MariaGtid(String gtid) {
        String[] gtidArr = gtid.split("-");
        this.domainId = NumberUtils.toLong(gtidArr[0]);
        this.serverId = NumberUtils.toLong(gtidArr[1]);
        this.sequence = NumberUtils.toLong(gtidArr[2]);
    }

    public static MariaGtid parse(String gtid) {
        return new MariaGtid(gtid);
    }

    public long getDomainId() {
        return domainId;
    }

    public void setDomainId(long domainId) {
        this.domainId = domainId;
    }

    public long getServerId() {
        return serverId;
    }

    public void setServerId(long serverId) {
        this.serverId = serverId;
    }

    public long getSequence() {
        return sequence;
    }

    public void setSequence(long sequence) {
        this.sequence = sequence;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MariaGtid mariaGtid = (MariaGtid) o;
        return domainId == mariaGtid.domainId &&
                serverId == mariaGtid.serverId &&
                sequence == mariaGtid.sequence;
    }

    @Override
    public String toString() {
        return String.format("%s-%s-%s", domainId, serverId, sequence);
    }
}
