package com.alibaba.otter.canal.prometheus.impl;

import com.alibaba.otter.canal.protocol.CanalPacket.PacketType;
import com.alibaba.otter.canal.server.netty.ClientInstanceProfiler;
import com.alibaba.otter.canal.server.netty.listener.ChannelFutureAggregator.ClientRequestResult;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;

import static com.alibaba.otter.canal.prometheus.CanalInstanceExports.DESTINATION;
import static com.alibaba.otter.canal.prometheus.CanalInstanceExports.DEST_LABELS;

/**
 * @author Chuanyi Li
 */
public class PrometheusClientInstanceProfiler implements ClientInstanceProfiler {

    private static final String PACKET_TYPE    = "canal_instance_client_packets";
    private static final String OUTBOUND_BYTES = "canal_instance_client_bytes";
    private static final String EMPTY_BATCHES  = "canal_instance_client_empty_batches";
    private static final String ERRORS         = "canal_instance_client_request_error";
    private static final String LATENCY        = "canal_instance_client_request_latency";
    private volatile boolean    running        = false;
    private final String       destination;
    private final String[]     destLabelValues;
    private final Counter      outboundCounter;
    private final Counter      packetsCounter;
    private final Counter      emptyBatchesCounter;
    private final Counter      errorsCounter;
    private final Histogram    responseLatency;

    public PrometheusClientInstanceProfiler(String destination) {
        this.destination = destination;
        this.destLabelValues = new String[]{destination};
        this.outboundCounter = Counter.build()
                .labelNames(DEST_LABELS)
                .name(OUTBOUND_BYTES)
                .help("Send bytes to client of instance " + destination)
                .create();
        this.packetsCounter = Counter.build()
                .labelNames(new String[]{DESTINATION, "packetType"})
                .name(PACKET_TYPE)
                .help("Send packets to client of instance " + destination)
                .create();
        this.emptyBatchesCounter = Counter.build()
                .labelNames(DEST_LABELS)
                .name(EMPTY_BATCHES)
                .help("Send empty batches to client of instance " + destination)
                .create();
        this.errorsCounter = Counter.build()
                .labelNames(new String[]{DESTINATION, "errorCode"})
                .name(ERRORS)
                .help("Client request errors of instance " + destination)
                .create();
        this.responseLatency = Histogram.build()
                .labelNames(DEST_LABELS)
                .name(LATENCY)
                .help("Client request latency of instance " + destination)
                // buckets in milliseconds
                .buckets(1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0)
                .create();
    }

    @Override
    public String getDestination() {
        return this.destination;
    }

    @Override
    public void profiling(ClientRequestResult result) {
        PacketType type = result.getType();
        outboundCounter.labels(destLabelValues).inc(result.getAmount());
        packetsCounter.labels(destination, type.name()).inc();
        short errorCode = result.getErrorCode();
        if (errorCode > 0) {
            errorsCounter.labels(destination, Short.toString(errorCode)).inc();
        }
        long latency = result.getLatency();
        responseLatency.labels(destLabelValues).observe(latency / 1000000);
        switch (type) {
            case GET:
                boolean empty = result.getEmpty();
                if (empty) {
                    emptyBatchesCounter.labels(destLabelValues).inc();
                }
                break;
            // reserve for others
            default:
                break;
        }
    }

    @Override public void start() {
        if (outboundCounter != null) {
            outboundCounter.register();
        }
        if (packetsCounter != null) {
            packetsCounter.register();
        }
        if (emptyBatchesCounter != null) {
            emptyBatchesCounter.register();
        }
        if (errorsCounter != null) {
            errorsCounter.register();
        }
        if (responseLatency != null) {
            responseLatency.register();
        }
        running = true;
    }

    @Override public void stop() {
        running = false;
        if (outboundCounter != null) {
            CollectorRegistry.defaultRegistry.unregister(outboundCounter);
        }
        if (packetsCounter != null) {
            CollectorRegistry.defaultRegistry.unregister(packetsCounter);
        }
        if (emptyBatchesCounter != null) {
            CollectorRegistry.defaultRegistry.unregister(emptyBatchesCounter);
        }
        if (errorsCounter != null) {
            CollectorRegistry.defaultRegistry.unregister(errorsCounter);
        }
        if (responseLatency != null) {
            CollectorRegistry.defaultRegistry.unregister(responseLatency);
        }
    }

    @Override public boolean isStart() {
        return running;
    }
}
