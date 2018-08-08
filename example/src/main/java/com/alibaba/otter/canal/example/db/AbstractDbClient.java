package com.alibaba.otter.canal.example.db;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import org.slf4j.MDC;

import java.util.Date;
import java.util.List;

public abstract class AbstractDbClient extends CanalConnectorClient {


    public abstract void insert(CanalEntry.Header header, List<CanalEntry.Column> afterColumns);

    public abstract void update(CanalEntry.Header header, List<CanalEntry.Column> afterColumns);

    public abstract void delete(CanalEntry.Header header, List<CanalEntry.Column> beforeColumns);


    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        super.start();
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        super.stop();
        MDC.remove("destination");
    }

    @Override
    protected void processMessage(Message message) {
        long batchId = message.getId();
        //遍历每条消息
        for (CanalEntry.Entry entry : message.getEntries()) {
            session(entry);//no exception
        }
        //ack all the time。
        connector.ack(batchId);
    }

    private void session(CanalEntry.Entry entry) {
        CanalEntry.EntryType entryType = entry.getEntryType();
        int times = 0;
        boolean success = false;
        while (!success) {
            if (times > 0) {
                /**
                 * 1:retry，重试，重试默认为3次，由retryTimes参数决定，如果重试次数达到阈值，则跳过，并且记录日志。
                 * 2:ignore,直接忽略，不重试，记录日志。
                 */
                if (exceptionStrategy == ExceptionStrategy.RETRY.code) {
                    if (times >= retryTimes) {
                        break;
                    }
                } else {
                    break;
                }
            }
            try {
                switch (entryType) {
                    case TRANSACTIONBEGIN:
                        transactionBegin(entry);
                        break;
                    case TRANSACTIONEND:
                        transactionEnd(entry);
                        break;
                    case ROWDATA:
                        rowData(entry);
                        break;
                    default:
                        break;
                }
                success = true;
            } catch (Exception e) {
                times++;
                logger.error("parse event has an error ,times: + " + times + ", data:" + entry.toString(), e);
            }

        }
    }

    private void rowData(CanalEntry.Entry entry) throws Exception {
        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
        CanalEntry.EventType eventType = rowChange.getEventType();
        CanalEntry.Header header = entry.getHeader();
        long executeTime = header.getExecuteTime();
        long delayTime = new Date().getTime() - executeTime;
        String sql = rowChange.getSql();

        try {
            if (!isDML(eventType) || rowChange.getIsDdl()) {
                processDDL(header, eventType, sql);
                return;
            }
            //处理DML数据
            processDML(header, eventType, rowChange, sql);
        } catch (Exception e) {
            logger.error("process event error ,", e);
            logger.error(rowFormat,
                    new Object[]{header.getLogfileName(), String.valueOf(header.getLogfileOffset()),
                            header.getSchemaName(), header.getTableName(), eventType,
                            String.valueOf(executeTime), String.valueOf(delayTime)});
            throw e;//重新抛出
        }
    }

    /**
     * 处理 dml 数据
     *
     * @param header
     * @param eventType
     * @param rowChange
     * @param sql
     */
    protected void processDML(CanalEntry.Header header, CanalEntry.EventType eventType, CanalEntry.RowChange rowChange, String sql) {
        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
            switch (eventType) {
                case DELETE:
                    delete(header, rowData.getBeforeColumnsList());
                    break;
                case INSERT:
                    insert(header, rowData.getAfterColumnsList());
                    break;
                case UPDATE:
                    update(header, rowData.getAfterColumnsList());
                    break;
                default:
                    whenOthers(header, sql);
            }
        }
    }

}




