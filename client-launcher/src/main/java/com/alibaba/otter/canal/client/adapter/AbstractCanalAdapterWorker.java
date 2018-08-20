package com.alibaba.otter.canal.client.adapter;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.support.Dml;
import com.alibaba.otter.canal.client.support.JdbcTypeUtil;
import com.alibaba.otter.canal.protocol.CanalEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * 适配器工作线程抽象类
 *
 * @author machengyuan 2018-8-19 下午11:30:49
 * @version 1.0.0
 */
public abstract class AbstractCanalAdapterWorker {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected String canalDestination;                              // canal实例
    protected List<List<CanalOuterAdapter>> canalOuterAdapters;     // 外部适配器
    protected ExecutorService groupInnerExecutorService;            // 组内工作线程池
    protected volatile boolean running = false;                     // 是否运行中
    protected Thread thread = null;
    protected Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
            logger.error("parse events has an error", e);
        }
    };

    /**
     * 将每个entry转换为DML操作对象并调用所有适配器写入
     *
     * @param entries
     */
    protected void convertAndWrite(List<CanalEntry.Entry> entries) {
        for (CanalEntry.Entry entry : entries) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            CanalEntry.RowChange rowChange;
            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(), e);
            }

            CanalEntry.EventType eventType = rowChange.getEventType();

            final Dml dml = new Dml();
            dml.setCanalDestination(canalDestination);
            dml.setDatabase(entry.getHeader().getSchemaName());
            dml.setTable(entry.getHeader().getTableName());
            dml.setType(eventType.toString());
            dml.setTs(System.currentTimeMillis());
            dml.setSql(rowChange.getSql());
            List<Map<String, Object>> data = new ArrayList<>();
            List<Map<String, Object>> old = new ArrayList<>();

            if (!rowChange.getIsDdl()) {
                Set<String> updateSet = new HashSet<>();
                for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    List<CanalEntry.Column> columns;

                    if (eventType == CanalEntry.EventType.DELETE) {
                        columns = rowData.getBeforeColumnsList();
                    } else {
                        columns = rowData.getAfterColumnsList();
                    }

                    for (CanalEntry.Column column : columns) {
                        row.put(column.getName(), JdbcTypeUtil.typeConvert(dml.getTable(), column.getName(), column.getValue(),
                                column.getSqlType(), column.getMysqlType()));
                        //获取update为true的字段
                        if (column.getUpdated()) {
                            updateSet.add(column.getName());
                        }
                    }
                    if (!row.isEmpty()) {
                        data.add(row);
                    }

                    if (eventType == CanalEntry.EventType.UPDATE) {
                        Map<String, Object> rowOld = new LinkedHashMap<>();
                        for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                            if (updateSet.contains(column.getName())) {
                                rowOld.put(column.getName(), JdbcTypeUtil.typeConvert(dml.getTable(), column.getName(),
                                        column.getValue(), column.getSqlType(), column.getMysqlType()));
                            }
                        }
                        // update操作将记录修改前的值
                        if (!rowOld.isEmpty()) {
                            old.add(rowOld);
                        }
                    }
                }
                if (!data.isEmpty()) {
                    dml.setData(data);
                }
                if (!old.isEmpty()) {
                    dml.setOld(old);
                }
            }
            List<Future<Boolean>> futures = new ArrayList<>();
            // 组间适配器并行运行
            for (List<CanalOuterAdapter> outerAdapters : canalOuterAdapters) {
                final List<CanalOuterAdapter> adapters = outerAdapters;
                futures.add(groupInnerExecutorService.submit(
                        new Callable<Boolean>() {
                            @Override
                            public Boolean call() {
                                boolean flag = true;
                                // 组内适配器穿行运行，尽量不要配置组内适配器
                                for (CanalOuterAdapter c : adapters) {
                                    long begin = System.currentTimeMillis();
                                    if (!c.writeOut(dml)) {
                                        logger.error("{} write fail！ data: {}", c.getClass().getName(), JSON.toJSONString(dml));

                                        flag = false;
                                    }
                                    if (logger.isDebugEnabled()) {
                                        logger.debug("{} elapsed time: {}", c.getClass().getName(), (System.currentTimeMillis() - begin));
                                    }
                                }
                                return flag;
                            }
                        })
                );

                // 等待所有适配器写入完成
                // 由于是组间并发操作，所以将阻塞直到耗时最久的工作组操作完成
                for (Future<Boolean> f : futures) {
                    try {
                        if (!f.get()) {
                            logger.error("Outer adapter write failed");
                        }
                    } catch (InterruptedException | ExecutionException e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }
        }
    }

    public abstract void start();

    public abstract void stop();
}