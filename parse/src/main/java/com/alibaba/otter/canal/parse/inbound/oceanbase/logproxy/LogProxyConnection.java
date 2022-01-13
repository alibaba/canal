package com.alibaba.otter.canal.parse.inbound.oceanbase.logproxy;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import javax.net.ssl.SSLException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.MultiStageCoprocessor;
import com.alibaba.otter.canal.parse.inbound.SinkFunction;
import com.alibaba.otter.canal.parse.inbound.oceanbase.OceanBaseConnection;
import com.oceanbase.clogproxy.client.LogProxyClient;
import com.oceanbase.clogproxy.client.config.ObReaderConfig;
import com.oceanbase.clogproxy.client.exception.LogProxyClientException;
import com.oceanbase.clogproxy.client.listener.RecordListener;
import com.oceanbase.oms.logmessage.DataMessage;
import com.oceanbase.oms.logmessage.LogMessage;

/**
 * 基于LogProxy的OceanBaseConnection实现
 *
 * @author wanghe Date: 2021/9/8 Time: 16:16
 */
public class LogProxyConnection implements OceanBaseConnection {

    private static final Logger logger = LoggerFactory.getLogger(LogProxyConnection.class);

    private final LogProxyClient client;
    private       boolean        startDump = false;

    public LogProxyConnection(InetSocketAddress clientAddress, ObReaderConfig logProxyConfig, SslConfig sslConfig) {
        SslContext sslContext;
        try {
            sslContext = sslConfig.sslContext();
        } catch (Throwable e) {
            throw new CanalParseException(e);
        }

        client = new LogProxyClient(clientAddress.getHostString(), clientAddress.getPort(), logProxyConfig, sslContext);
    }

    @Override
    public void connect() {
        // do nothing
    }

    @Override
    public void reconnect() {
        disconnect();
        connect();
    }

    @Override
    public void disconnect() {
        client.stop();
    }

    @Override
    public void dump(SinkFunction func) {
        client.addListener(new RecordListener() {

            @Override
            public void notify(LogMessage message) {
                if (shouldSkip(message)) {
                    return;
                }

                if (!func.sink(message)) {
                    client.stop();
                }
            }

            @Override
            public void onException(LogProxyClientException e) {
                if (e.needStop()) {
                    client.stop();
                }
                logger.error("OceanBase LogProxyClient listener error :", e);
            }
        });
        client.start();
        client.join();
    }

    @Override
    public void dump(MultiStageCoprocessor multiStageCoprocessor) {
        client.addListener(new RecordListener() {

            @Override
            public void notify(LogMessage record) {
                if (shouldSkip(record)) {
                    return;
                }

                if (!multiStageCoprocessor.publish(record)) {
                    client.stop();
                }
            }

            @Override
            public void onException(LogProxyClientException e) {
                if (e.needStop()) {
                    client.stop();
                }
                logger.error("OceanBase LogProxyClient listener error :", e);
            }
        });
        client.start();
        client.join();
    }

    private boolean shouldSkip(LogMessage message) {
        if (startDump) {
            return false;
        }

        if (message.getOpt() == DataMessage.Record.Type.BEGIN || message.getOpt() == DataMessage.Record.Type.DDL) {
            startDump = true;
            return false;
        }
        return true;
    }

    public static class SslConfig {

        private final boolean sslEnabled;
        private final String  trustCert;
        private final String  keyCert;
        private final String  key;

        public SslConfig(boolean sslEnabled, String trustCert, String keyCert, String key) {
            this.sslEnabled = sslEnabled;
            this.trustCert = trustCert;
            this.keyCert = keyCert;
            this.key = key;
        }

        public SslContext sslContext() throws FileNotFoundException, SSLException {
            if (!sslEnabled) {
                return null;
            }
            return SslContextBuilder.forClient()
                .trustManager(new FileInputStream(trustCert))
                .keyManager(new FileInputStream(keyCert), new FileInputStream(key))
                .build();
        }
    }
}
