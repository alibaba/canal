package com.alibaba.otter.canal.parse;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wanghe
 */
public interface ParserMetrics {

    /**
     * Get parallel status
     *
     * @return flag whether to process concurrently
     */
    boolean isParallel();

    /**
     * Get the blocking time in nanosecond
     *
     * @return accumulated blocking time
     */
    AtomicLong getEventsPublishBlockingTime();

    /**
     * Get the bytes of received binlog
     *
     * @return received binlog bytes
     */
    AtomicLong getReceivedBinlogBytes();
}
