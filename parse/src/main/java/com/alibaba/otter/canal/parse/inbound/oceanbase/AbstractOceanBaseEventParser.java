package com.alibaba.otter.canal.parse.inbound.oceanbase;

import java.io.IOException;

import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.AbstractEventParser;
import com.alibaba.otter.canal.parse.inbound.BinlogConnection;
import com.alibaba.otter.canal.parse.inbound.SinkFunction;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogPosition;

/**
 * OceanBase的CanalEventParser抽象实现类
 *
 * @param <EVENT> 接收到的日志数据类型
 * @author wanghe Date: 2021/9/8 Time: 16:16
 */
public abstract class AbstractOceanBaseEventParser<EVENT> extends AbstractEventParser<EVENT> {

    protected String  tenant;
    protected boolean excludeTenantInDbName;

    /**
     * 创建一个OceanBaseConnection实例
     *
     * @return OceanBaseConnection实例
     */
    protected abstract OceanBaseConnection buildOceanBaseConnection();

    protected EntryPosition findStartPosition() {
        LogPosition logPosition = logPositionManager.getLatestIndexBy(destination);
        if (logPosition == null) {
            return null;
        }
        return logPosition.getPostion();
    }

    @Override
    protected BinlogConnection buildBinlogConnection() {
        return buildOceanBaseConnection();
    }

    @Override
    protected void dump(BinlogConnection connection) throws IOException {
        if (!(connection instanceof OceanBaseConnection)) {
            throw new CanalParseException("Unsupported connection type : " + connection.getClass().getSimpleName());
        }
        OceanBaseConnection obConnection = (OceanBaseConnection) connection;
        obConnection.connect();
        if (parallel) {
            multiStageCoprocessor = buildMultiStageCoprocessor();
            multiStageCoprocessor.start();
            obConnection.dump(multiStageCoprocessor);
        } else {
            final SinkFunction<EVENT> sinkHandler = buildSinkHandler(null);
            obConnection.dump(sinkHandler);
        }
    }

    @Override
    protected void processSinkError(Throwable e, EntryPosition startPosition, EntryPosition lastPosition) {
        if (lastPosition != null) {
            logger.warn(String.format("ERROR ## parse this event has an error , last position : [%s]", lastPosition),
                e);
            return;
        }
        if (startPosition != null) {
            logger.warn(String.format("ERROR ## parse this event has an error , last position : [%s]", startPosition),
                e);
        }
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public void setExcludeTenantInDbName(boolean excludeTenantInDbName) {
        this.excludeTenantInDbName = excludeTenantInDbName;
    }
}
