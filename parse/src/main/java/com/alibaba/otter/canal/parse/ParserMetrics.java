package com.alibaba.otter.canal.parse;

import java.util.concurrent.atomic.AtomicLong;

public interface ParserMetrics {

    boolean isParallel();

    AtomicLong getEventsPublishBlockingTime();

    AtomicLong getReceivedBinlogBytes();
}
