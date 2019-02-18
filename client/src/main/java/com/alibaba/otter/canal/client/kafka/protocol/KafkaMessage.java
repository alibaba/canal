package com.alibaba.otter.canal.client.kafka.protocol;

import com.alibaba.otter.canal.protocol.Message;
import org.springframework.beans.BeanUtils;

/**
 * 消息对象（Kafka）
 *
 * @Author panjianping
 * @Email ipanjianping@qq.com
 * @Date 2018/12/17
 */
public class KafkaMessage extends Message {

    private static final long serialVersionUID = -293120358490119447L;

    /**
     * Kafka 消息 offset
     */
    private long              offset;

    public KafkaMessage(Message message, long offset){
        super(message.getId());
        BeanUtils.copyProperties(message, this);
        this.offset = offset;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
