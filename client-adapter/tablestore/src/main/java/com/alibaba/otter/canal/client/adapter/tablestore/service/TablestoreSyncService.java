package com.alibaba.otter.canal.client.adapter.tablestore.service;


import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;


import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter.Feature;
import com.alibaba.otter.canal.client.adapter.tablestore.enums.TablestoreFieldType;
import com.alibaba.otter.canal.client.adapter.tablestore.support.SyncUtil;
import com.alicloud.openservices.tablestore.TableStoreWriter;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.writer.WriterResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.otter.canal.client.adapter.tablestore.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import org.springframework.util.CollectionUtils;


/**
 * RDB同步操作业务
 *
 * @author rewerma 2018-11-7 下午06:45:49
 * @version 1.0.0
 */
public class TablestoreSyncService {

    private static final Logger               logger  = LoggerFactory.getLogger(TablestoreSyncService.class);

    private Map<String, Map<String, Integer>> columnsTypeCache;


    public Map<String, Map<String, Integer>> getColumnsTypeCache() {
        return columnsTypeCache;
    }

    public TablestoreSyncService(){
        this(new ConcurrentHashMap<>());
    }

    @SuppressWarnings("unchecked")
    public TablestoreSyncService(Map<String, Map<String, Integer>> columnsTypeCache){
        this.columnsTypeCache = columnsTypeCache;

    }

    public Future<WriterResult> sync(MappingConfig mappingConfig,
                     Dml dml,
                     TableStoreWriter writer) {

        List<RowChange> rowChanges = getRowChanges(dml, mappingConfig);
        if (CollectionUtils.isEmpty(rowChanges)) {
            return null;
        }

        Future<WriterResult> future = writer.addRowChangeWithFuture(rowChanges);

        if (logger.isDebugEnabled()) {
            logger.debug("DML: {}", JSON.toJSONString(dml, Feature.WriteNulls));
        }
        return future;
    }

    public List<RowChange> getRowChanges(Dml dml, MappingConfig config) {
        String type = dml.getType();
        boolean updateColume = config.getUpdateChangeColumns();
        if (type != null && type.equalsIgnoreCase("INSERT")) {
            return getInsertChanges(dml, updateColume, config);
        } else if (type != null && type.equalsIgnoreCase("UPDATE")) {
            return getUpdateChanges(dml, updateColume, config);
        } else if (type != null && type.equalsIgnoreCase("DELETE")) {
            return getDeleteChanges(dml, updateColume, config);
        } else {
            return null;
        }
    }

    /**
     * Update 类型下构造rowChangeList
     * @param dml
     * @param isColumnUpdate
     * @param config
     * @return
     */
    private List<RowChange> getUpdateChanges(Dml dml, boolean isColumnUpdate, MappingConfig config) {
        List<RowChange> changeList = new ArrayList<>();
        Map<String, String> columnMap = config.getDbMapping().getTargetColumnsParsed();
        Map<String, MappingConfig.ColumnItem> typeMap = SyncUtil.getTypeMap(config);
        MappingConfig.DbMapping dbMapping = config.getDbMapping();

        if (isColumnUpdate) {
            // 列更新
            for (int i = 0; i < dml.getData().size(); i++) {
                Map<String, Object> map = dml.getData().get(i);
                Map<String, Object> old = dml.getOld().get(i);
                boolean isPrimaryKeyChange = isPrimaryKeyChange(old, dbMapping.getTargetPk());
                if (isPrimaryKeyChange) {
                    // 如果发现主键修改用put delete操作
                    // 先 delete
                    RowUpdateChange change = new RowUpdateChange(dbMapping.getTargetTable());
                    PrimaryKey primaryKey = buildOldPrimaryKey(map, typeMap, columnMap, dbMapping.getTargetPk(), old);
                    change.setPrimaryKey(primaryKey);
                    for (Map.Entry<String, Object> entry : map.entrySet()) {
                        if (dbMapping.getTargetPk().containsKey(entry.getKey())) {
                            // 这是个主键, 不需要再次处理
                            continue;
                        }
                        if (!dbMapping.getTargetColumns().containsKey(entry.getKey())) {
                            // 可能是没有配置的字段
                            continue;
                        }
                        // 非主键
                        String targetColumn = columnMap.get(entry.getKey());
                        change.deleteColumns(targetColumn);
                    }
                    changeList.add(change);

                    // 然后再put
                    change = new RowUpdateChange(dbMapping.getTargetTable());
                    primaryKey = buildPrimaryKey(map, typeMap, columnMap, dbMapping.getTargetPk());
                    change.setPrimaryKey(primaryKey);

                    List<Column> columnList = getColumnsWhenPut(columnMap, dbMapping, map, typeMap);
                    if (!CollectionUtils.isEmpty(columnList)) {
                        change.put(columnList);
                        changeList.add(change);
                    }


                } else {
                    //否则用update
                    RowUpdateChange change = new RowUpdateChange(dbMapping.getTargetTable());
                    PrimaryKey primaryKey = buildPrimaryKey(map, typeMap, columnMap, dbMapping.getTargetPk());
                    change.setPrimaryKey(primaryKey);
                    // 部分update 部分delete

                    boolean validData = false;
                    for (Map.Entry<String, Object> entry : old.entrySet()) {
                        if (dbMapping.getTargetPk().containsKey(entry.getKey())) {
                            // 这是个主键, 不需要再次处理
                            continue;
                        }
                        if (!dbMapping.getTargetColumns().containsKey(entry.getKey())) {
                            // 可能是没有配置的字段
                            continue;
                        }
                        // 非主键
                        String targetColumn = columnMap.get(entry.getKey());
                        Object value = map.get(entry.getKey());
                        validData = true;
                        if (value == null) {
                            change.deleteColumns(targetColumn);
                        } else {
                            TablestoreFieldType type = typeMap.get(entry.getKey()).getType();
                            ColumnValue columnValue = SyncUtil.getColumnValue(value, type);
                            change.put(targetColumn, columnValue);
                        }
                    }
                    if (validData) {
                        changeList.add(change);
                    }

                }
            }

        } else {
            // 列覆盖
            for (int i = 0; i < dml.getData().size(); i++) {
                Map<String, Object> map = dml.getData().get(i);

                RowPutChange change = new RowPutChange(dbMapping.getTargetTable());
                PrimaryKey primaryKey = buildPrimaryKey(map, typeMap, columnMap, dbMapping.getTargetPk());
                change.setPrimaryKey(primaryKey);

                List<Column> columnList = getColumnsWhenPut(columnMap, dbMapping, map, typeMap);
                if (!CollectionUtils.isEmpty(columnList)) {
                    change.addColumns(columnList);
                    changeList.add(change);
                }

                if (dml.getOld() != null) {
                    // 如果主键发生修改，需要在tablestore中删除对应的原记录
                    Map<String, Object> old = dml.getOld().get(i);
                    if (isPrimaryKeyChange(old, dbMapping.getTargetPk())) {
                        RowDeleteChange delete = new RowDeleteChange(dbMapping.getTargetTable());
                        PrimaryKey primaryKeyDelete = buildOldPrimaryKey(map, typeMap, columnMap, dbMapping.getTargetPk(), old);
                        delete.setPrimaryKey(primaryKeyDelete);
                        changeList.add(delete);
                    }

                }

            }
        }

        return changeList;
    }

    /**
     * Delete 类型下构造rowChangeList
     * @param dml
     * @param isColumnUpdate
     * @param config
     * @return
     */
    private List<RowChange> getDeleteChanges(Dml dml, boolean isColumnUpdate, MappingConfig config) {
        List<RowChange> changeList = new ArrayList<>();
        Map<String, String> columnMap = config.getDbMapping().getTargetColumnsParsed();
        Map<String, MappingConfig.ColumnItem> typeMap = SyncUtil.getTypeMap(config);
        MappingConfig.DbMapping dbMapping = config.getDbMapping();

        if (isColumnUpdate) {
            // 列更新
            for (Map<String, Object> map : dml.getData()) {
                RowUpdateChange change = new RowUpdateChange(dbMapping.getTargetTable());
                PrimaryKey primaryKey = buildPrimaryKey(map, typeMap, columnMap, dbMapping.getTargetPk());
                change.setPrimaryKey(primaryKey);
                boolean validData = false;
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    if (dbMapping.getTargetPk().containsKey(entry.getKey())) {
                        // 这是个主键, 不需要再次处理
                        continue;
                    }
                    if (!dbMapping.getTargetColumns().containsKey(entry.getKey())) {
                        // 可能是没有配置的字段
                        continue;
                    }
                    // 非主键
                    validData = true;
                    String targetColumn = columnMap.get(entry.getKey());
                    change.deleteColumns(targetColumn);
                }
                if (validData) {
                    changeList.add(change);
                }
            }

        } else {
            // 列覆盖
            for (Map<String, Object> map : dml.getData()) {
                RowDeleteChange change = new RowDeleteChange(dbMapping.getTargetTable());
                PrimaryKey primaryKey = buildPrimaryKey(map, typeMap, columnMap, dbMapping.getTargetPk());
                change.setPrimaryKey(primaryKey);

                changeList.add(change);
            }
        }

        return changeList;
    }

    /**
     * Insert 类型下构造rowChangeList
     * @param dml
     * @param isColumnUpdate
     * @param config
     * @return
     */
    private List<RowChange> getInsertChanges(Dml dml, boolean isColumnUpdate, MappingConfig config) {
        List<RowChange> changeList = new ArrayList<>();

        Map<String, String> columnMap = config.getDbMapping().getTargetColumnsParsed();
        Map<String, MappingConfig.ColumnItem> typeMap = SyncUtil.getTypeMap(config);
        MappingConfig.DbMapping dbMapping = config.getDbMapping();
        if (isColumnUpdate) {
            // 列更新
            for (Map<String, Object> map : dml.getData()) {
                RowUpdateChange change = new RowUpdateChange(dbMapping.getTargetTable());
                PrimaryKey primaryKey = buildPrimaryKey(map, typeMap, columnMap, dbMapping.getTargetPk());
                change.setPrimaryKey(primaryKey);

                List<Column> columnList = getColumnsWhenPut(columnMap, dbMapping, map, typeMap);
                if (!CollectionUtils.isEmpty(columnList)) {
                    change.put(columnList);
                    changeList.add(change);
                }
            }

        } else {
            // 列覆盖
            for (Map<String, Object> map : dml.getData()) {
                RowPutChange change = new RowPutChange(dbMapping.getTargetTable());
                PrimaryKey primaryKey = buildPrimaryKey(map, typeMap, columnMap, dbMapping.getTargetPk());
                change.setPrimaryKey(primaryKey);

                List<Column> columnList = getColumnsWhenPut(columnMap, dbMapping, map, typeMap);
                if (!CollectionUtils.isEmpty(columnList)) {
                    change.addColumns(columnList);
                }

                changeList.add(change);
            }
        }
        return changeList;
    }


    /**
     * 组装rowChange的主键
     * @param map 数据map
     * @param typeMap  类型映射
     * @param columnMap 字段名称映射
     * @param targetPk  主键map
     * @return
     */
    private PrimaryKey buildPrimaryKey(Map<String, Object> map, Map<String, MappingConfig.ColumnItem> typeMap, Map<String, String> columnMap, LinkedHashMap<String, String> targetPk) {
        List primaryKeyList = new ArrayList<>();
        for (Map.Entry<String, String> entry : targetPk.entrySet()) {
            // build primary key
            String targetColumn = columnMap.get(entry.getKey());
            Object value = map.get(entry.getKey());
            TablestoreFieldType type = typeMap.get(entry.getKey()).getType();
            PrimaryKeyValue keyValue = SyncUtil.getPrimaryKeyValue(value, type);
            PrimaryKeyColumn primaryKeyColumn = new PrimaryKeyColumn(targetColumn, keyValue);
            primaryKeyList.add(primaryKeyColumn);
        }
        return new PrimaryKey(primaryKeyList);
    }



    private PrimaryKey buildOldPrimaryKey(Map<String, Object> map,
                                          Map<String, MappingConfig.ColumnItem> typeMap,
                                          Map<String, String> columnMap,
                                          LinkedHashMap<String, String> targetPk,
                                          Map<String, Object> old) {
        List primaryKeyList = new ArrayList<>();
        for (Map.Entry<String, String> entry : targetPk.entrySet()) {
            // build primary key
            String targetColumn = columnMap.get(entry.getKey());
            Object value = old != null && old.containsKey(entry.getKey()) && !old.get(entry.getKey()).equals(map.get(entry.getKey())) ? old.get(entry.getKey()) : map.get(entry.getKey());
            TablestoreFieldType type = typeMap.get(entry.getKey()).getType();
            PrimaryKeyValue keyValue = SyncUtil.getPrimaryKeyValue(value, type);
            PrimaryKeyColumn primaryKeyColumn = new PrimaryKeyColumn(targetColumn, keyValue);
            primaryKeyList.add(primaryKeyColumn);
        }
        return new PrimaryKey(primaryKeyList);
    }

    /**
     * 用于获得全量覆盖时的非主键列对应的columnlist
     * @param columnMap
     * @param dbMapping
     * @param map
     * @param typeMap
     * @return
     */
    private List<Column> getColumnsWhenPut(Map<String, String> columnMap,
                                           MappingConfig.DbMapping dbMapping,
                                           Map<String, Object> map,
                                           Map<String, MappingConfig.ColumnItem> typeMap) {
        List<Column> columnList = new ArrayList<>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (dbMapping.getTargetPk().containsKey(entry.getKey())) {
                // 这是个主键, 不需要再次处理
                continue;
            }
            if (!dbMapping.getTargetColumns().containsKey(entry.getKey())) {
                // 可能是没有配置的字段
                continue;
            }
            // 非主键
            String targetColumn = columnMap.get(entry.getKey());
            Object value = entry.getValue();
            if (value == null) {
                // insert时空值过滤掉
                continue;
            }
            TablestoreFieldType type = typeMap.get(entry.getKey()).getType();
            ColumnValue columnValue = SyncUtil.getColumnValue(value, type);
            columnList.add(new Column(targetColumn, columnValue));
        }
        return columnList;
    }

    /**
     * 检查是否主键被修改
     * @param old       old中的数据
     * @param targetPk  主键map
     * @return
     */
    private boolean isPrimaryKeyChange(Map<String, Object> old, Map<String, String> targetPk) {
        for (String pkCol : targetPk.keySet()) {
            if (old.containsKey(pkCol)) {
                return true;
            }
        }
        return false;
    }

    public void close() {

    }
}
