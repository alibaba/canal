package com.alibaba.otter.canal.parse.inbound;

import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.protocol.CanalEntry;

/**
 * 解析binlog的接口
 *
 * @author yuanzu Date: 12-9-20 Time: 下午8:46
 */
public interface BinlogParser<T> extends CanalLifeCycle {

    /**
     * 数据解析方法，将对应类型的event转为CanalEntry.Entry格式
     *
     * @param event  原始数据
     * @param isSeek 是否是seek函数调用，对应MySQL处理器中的seek方法
     * @return 转化成的CanalEntry.Entry
     * @throws CanalParseException 封装后的解析异常
     */
    CanalEntry.Entry parse(T event, boolean isSeek) throws CanalParseException;

    /**
     * 重置解析器状态
     */
    void reset();
}
