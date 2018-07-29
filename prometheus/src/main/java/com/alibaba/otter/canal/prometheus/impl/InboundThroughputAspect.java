package com.alibaba.otter.canal.prometheus.impl;

import io.prometheus.client.Collector;
import io.prometheus.client.CounterMetricFamily;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.jctools.maps.ConcurrentAutoTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Chuanyi Li
 */
@Aspect
public class InboundThroughputAspect {

    private static final Logger              logger    = LoggerFactory.getLogger(InboundThroughputAspect.class);

    /**
     *  Support highly scalable counters
     *  @see ConcurrentAutoTable
     */
    private static final ConcurrentAutoTable total     = new ConcurrentAutoTable();

    private static final Collector           collector = new InboundThroughputCollector();

    public static Collector getCollector() {
        return collector;
    }

    @Pointcut("call(byte[] com.alibaba.otter.canal.parse.driver.mysql.socket.SocketChannel.read(..))")
    public void read() {}

    @Pointcut("call(void com.alibaba.otter.canal.parse.driver.mysql.socket.SocketChannel.read(..)) ")
    public void readBytes() {}

    //nested read, just eliminate them.
    @Pointcut("withincode(* com.alibaba.otter.canal.parse.driver.mysql.socket.SocketChannel.read(..))")
    public void nestedCall() {}

    @After("read() && !nestedCall()  && args(len, ..)")
    public void recordRead(int len) {
        accumulateBytes(len);
    }

    @After("readBytes() && !nestedCall() && args(.., len, timeout)")
    public void recordReadBytes(int len, int timeout) {
        accumulateBytes(len);
    }

    private void accumulateBytes(int count) {
        try {
            total.add(count);
        } catch (Throwable t) {
            //Catch every Throwable, rather than break the business logic.
            logger.warn("Error while accumulate inbound bytes.", t);
        }
    }

    public static class InboundThroughputCollector extends Collector {

        private InboundThroughputCollector() {}

        @Override
        public List<MetricFamilySamples> collect() {
            List<MetricFamilySamples> mfs = new ArrayList<MetricFamilySamples>();
            CounterMetricFamily bytes = new CounterMetricFamily("canal_net_inbound_bytes",
                    "Total socket inbound bytes of canal server.",
                    total.get());
            mfs.add(bytes);
            return mfs;
        }
    }

}
