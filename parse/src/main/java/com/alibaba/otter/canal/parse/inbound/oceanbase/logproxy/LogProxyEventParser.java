package com.alibaba.otter.canal.parse.inbound.oceanbase.logproxy;

import java.util.List;

import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.parse.inbound.AbstractBinlogParser;
import com.alibaba.otter.canal.parse.inbound.MultiStageCoprocessor;
import com.alibaba.otter.canal.parse.inbound.oceanbase.AbstractOceanBaseEventParser;
import com.alibaba.otter.canal.parse.inbound.oceanbase.OceanBaseConnection;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.oceanbase.clogproxy.client.config.ObReaderConfig;
import com.oceanbase.clogproxy.client.message.LogMessage;

/**
 * 基于LogProxy的CanalEventParser实现
 *
 * @author wanghe Date: 2021/9/8 Time: 16:16
 */
public class LogProxyEventParser extends AbstractOceanBaseEventParser<LogMessage> implements CanalEventParser<LogMessage> {

    private AuthenticationInfo           logProxyInfo;
    private ObReaderConfig               logProxyConfig;
    /**
     * config to build SslContext
     */
    private LogProxyConnection.SslConfig sslConfig;

    @Override
    public void start() {
        if (runningInfo == null) {
            runningInfo = logProxyInfo;
        }
        super.start();
    }

    @Override
    protected OceanBaseConnection buildOceanBaseConnection() {
        // priority of start position source: position manager > properties file > zero value
        EntryPosition startPosition = findStartPosition();
        if (startPosition != null) {
            logProxyConfig.setStartTimestamp(startPosition.getTimestamp());
        }
        logger.info("Build connection with config {}", logProxyConfig.toString());
        return new LogProxyConnection(logProxyInfo.getAddress(), logProxyConfig, sslConfig);
    }

    @Override
    protected MultiStageCoprocessor buildMultiStageCoprocessor() {
        if (parallelThreadSize == null) {
            parallelThreadSize = Runtime.getRuntime().availableProcessors() * 60 / 100;
        }
        LogProxyMultiStageCoprocessor coprocessor = new LogProxyMultiStageCoprocessor((LogProxyMessageParser) binlogParser,
            parallelThreadSize,
            transactionBuffer,
            parallelBufferSize,
            destination);
        coprocessor.setEventsPublishBlockingTime(eventsPublishBlockingTime);
        return coprocessor;
    }

    @Override
    protected AbstractBinlogParser<LogMessage> buildParser() {
        LogProxyMessageParser parser = new LogProxyMessageParser();
        parser.setFilterDmlInsert(filterDmlInsert);
        parser.setFilterDmlUpdate(filterDmlUpdate);
        parser.setFilterDmlDelete(filterDmlDelete);
        parser.setTenant(tenant);
        parser.setExcludeTenantInDbName(excludeTenantInDbName);
        parser.setReceivedMessageCount(receivedBinlogCount);
        return parser;
    }

    @Override
    protected LogPosition buildLastTransactionPosition(List<CanalEntry.Entry> entries) {
        LogPosition position = super.buildLastTransactionPosition(entries);
        if (position != null) {
            for (CanalEntry.Entry entry : entries) {
                if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN) {
                    position.getPostion().setGtid(entry.getHeader().getGtid());
                    break;
                }
            }
        }
        return position;
    }

    public void setLogProxyConfig(ObReaderConfig logProxyConfig) {
        this.logProxyConfig = logProxyConfig;
    }

    public void setLogProxyInfo(AuthenticationInfo logProxyInfo) {
        this.logProxyInfo = logProxyInfo;
    }

    public void setSslConfig(LogProxyConnection.SslConfig sslConfig) {
        this.sslConfig = sslConfig;
    }
}
