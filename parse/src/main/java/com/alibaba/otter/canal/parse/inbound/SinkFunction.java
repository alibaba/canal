package com.alibaba.otter.canal.parse.inbound;

/**
 * receive parsed bytes , 用于处理要解析的数据块
 * 
 * @author: yuanzu Date: 12-9-20 Time: 下午2:50
 */

public interface SinkFunction<EVENT> {

    public boolean sink(EVENT event);
}
