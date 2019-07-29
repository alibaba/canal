package com.alibaba.otter.canal.protocol.position;

import java.io.Serializable;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.alibaba.otter.canal.common.utils.CanalToStringStyle;

/**
 * 描述一个position范围
 * 
 * @author jianghang 2012-7-10 下午05:28:38
 * @version 1.0.0
 */
public class PositionRange<T extends Position> implements Serializable {

    private static final long serialVersionUID = -9162037079815694784L;
    private T                 start;
    // add by ljh at 2012-09-05，用于记录一个可被ack的位置，保证每次提交到cursor中的位置是一个完整事务的结束
    private T                 ack;
    private T                 end;
    // add by ljh at 2019-06-25，用于精确记录ringbuffer中的位点
    private Long              endSeq           = -1L;

    public PositionRange(){
    }

    public PositionRange(T start, T end){
        this.start = start;
        this.end = end;
    }

    public T getStart() {
        return start;
    }

    public void setStart(T start) {
        this.start = start;
    }

    public T getEnd() {
        return end;
    }

    public void setEnd(T end) {
        this.end = end;
    }

    public T getAck() {
        return ack;
    }

    public void setAck(T ack) {
        this.ack = ack;
    }

    public Long getEndSeq() {
        return endSeq;
    }

    public void setEndSeq(Long endSeq) {
        this.endSeq = endSeq;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((ack == null) ? 0 : ack.hashCode());
        result = prime * result + ((end == null) ? 0 : end.hashCode());
        result = prime * result + ((start == null) ? 0 : start.hashCode());
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
        if (!(obj instanceof PositionRange)) {
            return false;
        }
        PositionRange other = (PositionRange) obj;
        if (ack == null) {
            if (other.ack != null) {
                return false;
            }
        } else if (!ack.equals(other.ack)) {
            return false;
        }
        if (end == null) {
            if (other.end != null) {
                return false;
            }
        } else if (!end.equals(other.end)) {
            return false;
        }
        if (start == null) {
            if (other.start != null) {
                return false;
            }
        } else if (!start.equals(other.start)) {
            return false;
        }
        return true;
    }

}
