package com.alibaba.otter.canal.prometheus.impl;

import io.prometheus.client.Collector;
import io.prometheus.client.CounterMetricFamily;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.jboss.netty.channel.Channel;
import org.jctools.maps.ConcurrentAutoTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.otter.canal.server.netty.NettyUtils.HEADER_LENGTH;

/**
 * @author Chuanyi Li
 */
@Aspect
public class OutboundThroughputAspect {
    private static final Logger              logger    = LoggerFactory.getLogger(OutboundThroughputAspect.class);

    /**
     *  Support highly scalable counters
     *  @see ConcurrentAutoTable
     */
    private static final ConcurrentAutoTable total     = new ConcurrentAutoTable();

    private static final Collector           collector = new OutboundThroughputCollector();

    public static Collector getCollector() {
        return collector;
    }

    @Pointcut("call(* com.alibaba.otter.canal.server.netty.NettyUtils.write(..))")
    public void write() {}

    //nested read, just eliminate them.
    @Pointcut("withincode(* com.alibaba.otter.canal.server.netty.NettyUtils.write(..))")
    public void nestedCall() {}

    @After("write() && !nestedCall() && args(ch, bytes, ..)")
    public void recordWriteBytes(Channel ch, byte[] bytes) {
        if (bytes != null) {
            accumulateBytes(HEADER_LENGTH + bytes.length);
        }
    }

    @After("write() && !nestedCall() && args(ch, buf, ..)")
    public void recordWriteBuffer(Channel ch, ByteBuffer buf) {
        if (buf != null) {
            total.add(HEADER_LENGTH + buf.limit());
        }
    }
    private void accumulateBytes(int count) {
        try {
            total.add(count);
        } catch (Throwable t) {
            //Catch every Throwable, rather than break the business logic.
            logger.warn("Error while accumulate inbound bytes.", t);
        }
    }

    public static class OutboundThroughputCollector extends Collector {

        private OutboundThroughputCollector() {}

        @Override public List<MetricFamilySamples> collect() {
            List<MetricFamilySamples> mfs = new ArrayList<MetricFamilySamples>();
            CounterMetricFamily bytes = new CounterMetricFamily("canal_net_outbound_bytes",
                    "Total socket outbound bytes of canal server.",
                    total.get());
            mfs.add(bytes);
            return mfs;
        }
    }
}
