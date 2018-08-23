package com.alibaba.otter.canal.client.kafka.running;

/**
 * client running状态信息
 *
 * @author machengyuan 2018-06-20 下午04:10:12
 * @version 1.0.0
 */
public class ClientRunningData {

    private String  groupId;
    private String  address;
    private boolean active = true;

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

}
