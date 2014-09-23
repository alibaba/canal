package com.alibaba.otter.canal.protocol.position;

/**
 * 基于时间的位置，position数据不唯一
 * 
 * @author jianghang 2012-6-14 下午09:22:04
 * @version 1.0.0
 */
public class TimePosition extends Position {

    private static final long serialVersionUID = 6185261261064226380L;
    protected Long            timestamp;

    public TimePosition(Long timestamp){
        this.timestamp = timestamp;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((timestamp == null) ? 0 : timestamp.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof TimePosition)) {
            return false;
        }
        TimePosition other = (TimePosition) obj;
        if (timestamp == null) {
            if (other.timestamp != null) {
                return false;
            }
        } else if (!timestamp.equals(other.timestamp)) {
            return false;
        }
        return true;
    }

}
