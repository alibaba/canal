package com.alibaba.otter.canal.parse;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wanghe
 */
public interface ParserMetrics {

    /**
     * 获取并发情况
     *
     * @return 返回是否并发执行
     */
    boolean isParallel();

    /**
     * 获取并发处理器中publish操作的阻塞时间
     *
     * @return 累计阻塞的时间，单位纳秒
     */
    AtomicLong getEventsPublishBlockingTime();

    /**
     * 获取接收到的Binlog数量
     *
     * @return 接收到的Binlog数量
     */
    AtomicLong getReceivedBinlogCount();
}
