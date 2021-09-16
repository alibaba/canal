package com.alibaba.otter.canal.client.adapter.tablestore.service;


import com.alibaba.otter.canal.client.adapter.support.*;
import com.alibaba.otter.canal.client.adapter.tablestore.TablestoreAdapter;
import com.alibaba.otter.canal.client.adapter.tablestore.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.tablestore.support.SyncUtil;
import com.alicloud.openservices.tablestore.TableStoreWriter;
import com.alicloud.openservices.tablestore.model.RowChange;
import com.alicloud.openservices.tablestore.writer.WriterResult;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class TablestoreEtlService extends AbstractEtlService {

    private TableStoreWriter writer;
    private MappingConfig config;
    private TablestoreSyncService syncService;


    public TablestoreEtlService(TableStoreWriter writer, MappingConfig config){
        super("Tablestore", config);
        this.writer = writer;
        this.config = config;
        syncService = new TablestoreSyncService();
    }


    public EtlResult importData(List<String> params) {
        MappingConfig.DbMapping dbMapping = config.getDbMapping();
        String sql = "SELECT * FROM " + SyncUtil.getDbTableName(dbMapping.getDatabase(), dbMapping.getTable());
        return importData(sql, params);
    }

    @Override
    protected boolean executeSqlImport(DataSource srcDS, String sql, List<Object> values,
                                       AdapterConfig.AdapterMapping mapping, AtomicLong impCount, List<String> errMsg) {

        try {
            MappingConfig.DbMapping dbMapping = (MappingConfig.DbMapping) mapping;
            Map<String, String> columnsMap = dbMapping.getTargetColumnsParsed();

            Util.sqlRS(srcDS, sql, values, rs -> {
                int idx = 0;
                List<Future<WriterResult>> futureList = new ArrayList<>();
                while (true) {
                    try {
                        if (!rs.next()) break;
                    } catch (SQLException throwables) {
                        logger.error("Error while get data from srcDs", throwables);
                        break;
                    }

                    Dml dml = getDMLByRs(columnsMap, rs);

                    List<RowChange> rowChanges = syncService.getRowChanges(dml, config);
                    if (CollectionUtils.isEmpty(rowChanges)) {
                        return null;
                    }


                    Future<WriterResult> future = writer.addRowChangeWithFuture(rowChanges);
                    if (future != null) {
                        futureList.add(future);
                    }

                }
                writer.flush();
                for (Future<WriterResult> future : futureList) {
                    try {
                        WriterResult result = future.get();
                        if (result != null && result.isAllSucceed()) {
                            impCount.incrementAndGet();
                            idx++;
                        } else if (result != null && !result.isAllSucceed()) {
                            List<WriterResult.RowChangeStatus> totalFailedRows = result.getFailedRows();
                            List<String> msgs = totalFailedRows.stream().map(e -> TablestoreAdapter.buildErrorMsgForFailedRowChange(e)).collect(Collectors.toList());
                            logger.error("Failed rows when ETL:" + org.springframework.util.StringUtils.collectionToDelimitedString(msgs, ",", "[", "]"));
                        }
                    } catch (InterruptedException e) {
                        logger.info("InterruptedException", e);
                        errMsg.add(e.getMessage());
                        Thread.currentThread().interrupt();
                    } catch (ExecutionException e) {
                        errMsg.add(e.getMessage());
                        throw new RuntimeException(e);
                    }
                }

                return idx;
            });
            return true;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        }
    }




    private Dml getDMLByRs(Map<String, String> columnsMap, ResultSet rs) {
        try {
            Dml dml = new Dml();
            dml.setType("INSERT");
            Map<String, Object> dataMap = new HashMap<>();
            List<Map<String, Object>> dataList = new ArrayList<>();
            dataList.add(dataMap);
            dml.setData(dataList);
            for (String key : columnsMap.keySet()) {
                dataMap.put(key, rs.getObject(key));
            }

            return dml;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }




}
