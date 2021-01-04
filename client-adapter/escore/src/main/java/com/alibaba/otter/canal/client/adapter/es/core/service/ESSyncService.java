package com.alibaba.otter.canal.client.adapter.es.core.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.otter.canal.client.adapter.es.core.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.core.config.ESSyncConfig.ESMapping;
import com.alibaba.otter.canal.client.adapter.es.core.config.ESSyncConfig.ObjField;
import com.alibaba.otter.canal.client.adapter.es.core.config.ESSyncConfig.ObjFieldType;
import com.alibaba.otter.canal.client.adapter.es.core.config.ESSyncConfig.ObjFields;
import com.alibaba.otter.canal.client.adapter.es.core.config.SchemaItem;
import com.alibaba.otter.canal.client.adapter.es.core.config.SchemaItem.ColumnItem;
import com.alibaba.otter.canal.client.adapter.es.core.config.SchemaItem.FieldItem;
import com.alibaba.otter.canal.client.adapter.es.core.config.SchemaItem.RelationFieldsPair;
import com.alibaba.otter.canal.client.adapter.es.core.config.SchemaItem.TableItem;
import com.alibaba.otter.canal.client.adapter.es.core.config.SqlParser;
import com.alibaba.otter.canal.client.adapter.es.core.support.ESSyncUtil;
import com.alibaba.otter.canal.client.adapter.es.core.support.ESTemplate;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.Util;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * ES 同步 Service
 *
 * @author rewerma 2018-11-01
 * @version 1.0.0
 */
public class ESSyncService {

    private static Logger logger = LoggerFactory.getLogger(ESSyncService.class);

    private ESTemplate    esTemplate;

    public ESSyncService(ESTemplate esTemplate){
        this.esTemplate = esTemplate;
    }

    public void sync(Collection<ESSyncConfig> esSyncConfigs, Dml dml) {
        long begin = System.currentTimeMillis();
        if (esSyncConfigs != null) {
            if (logger.isTraceEnabled()) {
                logger.trace("Destination: {}, database:{}, table:{}, type:{}, affected index count: {}",
                    dml.getDestination(),
                    dml.getDatabase(),
                    dml.getTable(),
                    dml.getType(),
                    esSyncConfigs.size());
            }

            for (ESSyncConfig config : esSyncConfigs) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Prepared to sync index: {}, destination: {}",
                        config.getEsMapping().get_index(),
                        dml.getDestination());
                }
                this.sync(config, dml);
                if (logger.isTraceEnabled()) {
                    logger.trace("Sync completed: {}, destination: {}",
                        config.getEsMapping().get_index(),
                        dml.getDestination());
                }
            }
            if (logger.isTraceEnabled()) {
                logger.trace("Sync elapsed time: {} ms, affected indexes count：{}, destination: {}",
                    (System.currentTimeMillis() - begin),
                    esSyncConfigs.size(),
                    dml.getDestination());
            }
            if (logger.isDebugEnabled()) {
                StringBuilder configIndexes = new StringBuilder();
                esSyncConfigs
                    .forEach(esSyncConfig -> configIndexes.append(esSyncConfig.getEsMapping().get_index()).append(" "));
                logger.debug("DML: {} \nAffected indexes: {}",
                    JSON.toJSONString(dml, SerializerFeature.WriteMapNullValue),
                    configIndexes.toString());
            }
        }
    }

    public void sync(ESSyncConfig config, Dml dml) {
        try {
            // 如果是按时间戳定时更新则返回
            if (config.getEsMapping().isSyncByTimestamp()) {
                return;
            }

            long begin = System.currentTimeMillis();

            String type = dml.getType();
            if (type != null && type.equalsIgnoreCase("INSERT")) {
                insert(config, dml);
            } else if (type != null && type.equalsIgnoreCase("UPDATE")) {
                update(config, dml);
            } else if (type != null && type.equalsIgnoreCase("DELETE")) {
                delete(config, dml);
            } else {
                return;
            }

            if (logger.isTraceEnabled()) {
                logger.trace("Sync elapsed time: {} ms,destination: {}, es index: {}",
                    (System.currentTimeMillis() - begin),
                    dml.getDestination(),
                    config.getEsMapping().get_index());
            }
        } catch (Throwable e) {
            logger.error("sync error, es index: {}, DML : {}", config.getEsMapping().get_index(), dml);
            throw new RuntimeException(e);
        }
    }

    /**
     * 插入操作dml
     *
     * @param config es配置
     * @param dml dml数据
     */
    private void insert(ESSyncConfig config, Dml dml) {
        List<Map<String, Object>> dataList = dml.getData();
        if (dataList == null || dataList.isEmpty()) {
            return;
        }
        SchemaItem schemaItem = config.getEsMapping().getSchemaItem();

        //dml涉及表名
        String dmlTableName = dml.getTable();
        //主表名
        String mainTableName = schemaItem.getMainTable().getTableName();

        for (Map<String, Object> data : dataList) {
            if (data == null || data.isEmpty()) {
                continue;
            }

            if (schemaItem.getAliasTableItems().size() == 1 && schemaItem.isAllFieldsSimple()) {
                // ------单表 & 所有字段都为简单字段------
                if (mainTableName.equalsIgnoreCase(dmlTableName)) {
                    singleTableSimpleFiledInsert(config, dml, data);
                } else {
                    // 对象字段涉及的子表数据插入
                    updateObjectFields(config, dml, data, null);
                }
            } else {
                // ------是主表 查询sql来插入------
                if (mainTableName.equalsIgnoreCase(dmlTableName)) {
                    mainTableInsert(config, dml, data);
                } else {
                    // 对象字段涉及的子表数据插入
                    updateObjectFields(config, dml, data, null);
                }

                // 从表的操作
                for (TableItem tableItem : schemaItem.getAliasTableItems().values()) {
                    if (tableItem.isMain()) {
                        continue;
                    }
                    if (!tableItem.getTableName().equals(dmlTableName)) {
                        continue;
                    }
                    // 关联条件出现在主表查询条件是否为简单字段
                    boolean allFieldsSimple = true;
                    for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                        if (fieldItem.isMethod() || fieldItem.isBinaryOp()) {
                            allFieldsSimple = false;
                            break;
                        }
                    }
                    // 所有查询字段均为简单字段
                    if (allFieldsSimple) {
                        // 不是子查询
                        if (!tableItem.isSubQuery()) {
                            // ------关联表简单字段插入------
                            Map<String, Object> esFieldData = new LinkedHashMap<>();
                            for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                                Object value = esTemplate.getValFromData(config.getEsMapping(),
                                    data,
                                    fieldItem.getFieldName(),
                                    fieldItem.getColumn().getColumnName());
                                esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), value);
                            }

                            joinTableSimpleFieldOperation(config, dml, data, tableItem, esFieldData);
                        } else {
                            // ------关联子表简单字段插入------
                            subTableSimpleFieldOperation(config, dml, data, null, tableItem);
                        }
                    } else {
                        // ------关联子表复杂字段插入 执行全sql更新es------
                        wholeSqlOperation(config, dml, data, null, tableItem);
                    }
                }
            }
        }
    }

    /**
     * 更新操作dml
     *
     * @param config es配置
     * @param dml dml数据
     */
    private void update(ESSyncConfig config, Dml dml) {
        List<Map<String, Object>> dataList = dml.getData();
        List<Map<String, Object>> oldList = dml.getOld();
        if (dataList == null || dataList.isEmpty() || oldList == null || oldList.isEmpty()) {
            return;
        }
        SchemaItem schemaItem = config.getEsMapping().getSchemaItem();

        //dml涉及表名
        String dmlTableName = dml.getTable();
        //主数据表名
        String mainTableName = schemaItem.getMainTable().getTableName();

        int i = 0;
        for (Map<String, Object> data : dataList) {
            Map<String, Object> old = oldList.get(i);
            if (data == null || data.isEmpty() || old == null || old.isEmpty()) {
                continue;
            }

            if (schemaItem.getAliasTableItems().size() == 1 && schemaItem.isAllFieldsSimple()) {
                // ------单表 & 所有字段都为简单字段------
                if (mainTableName.equalsIgnoreCase(dmlTableName)) {
                    singleTableSimpleFiledUpdate(config, dml, data, old);
                } else {
                    // 对象字段涉及的子表数据更新
                    updateObjectFields(config, dml, data, old);
                }
            } else {
                // ------主表 查询sql来更新------
                if (mainTableName.equalsIgnoreCase(dmlTableName)) {
                    ESMapping mapping = config.getEsMapping();
                    String idFieldName = mapping.get_id() == null ? mapping.getPk() : mapping.get_id();
                    FieldItem idFieldItem = schemaItem.getSelectFields().get(idFieldName);

                    boolean idFieldSimple = true;
                    if (idFieldItem.isMethod() || idFieldItem.isBinaryOp()) {
                        idFieldSimple = false;
                    }

                    boolean allUpdateFieldSimple = true;
                    out: for (FieldItem fieldItem : schemaItem.getSelectFields().values()) {
                        for (ColumnItem columnItem : fieldItem.getColumnItems()) {
                            if (old.containsKey(columnItem.getColumnName())) {
                                if (fieldItem.isMethod() || fieldItem.isBinaryOp()) {
                                    allUpdateFieldSimple = false;
                                    break out;
                                }
                            }
                        }
                    }

                    // 不支持主键更新!!

                    // 判断是否有外键更新
                    boolean fkChanged = false;
                    for (TableItem tableItem : schemaItem.getAliasTableItems().values()) {
                        if (tableItem.isMain()) {
                            continue;
                        }
                        boolean changed = false;
                        for (List<FieldItem> fieldItems : tableItem.getRelationTableFields().values()) {
                            for (FieldItem fieldItem : fieldItems) {
                                if (old.containsKey(fieldItem.getColumn().getColumnName())) {
                                    fkChanged = true;
                                    changed = true;
                                    break;
                                }
                            }
                        }
                        // 如果外键有修改,则更新所对应该表的所有查询条件数据
                        if (changed) {
                            for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                                fieldItem.getColumnItems()
                                    .forEach(columnItem -> old.put(columnItem.getColumnName(), null));
                            }
                        }
                    }

                    // 判断主键和所更新的字段是否全为简单字段
                    if (idFieldSimple && allUpdateFieldSimple && !fkChanged) {
                        singleTableSimpleFiledUpdate(config, dml, data, old);
                    } else {
                        mainTableUpdate(config, dml, data, old);
                    }
                } else {
                    // 对象字段涉及的子表数据更新
                    updateObjectFields(config, dml, data, old);
                }


                // 从表的操作
                for (TableItem tableItem : schemaItem.getAliasTableItems().values()) {
                    if (tableItem.isMain()) {
                        continue;
                    }
                    if (!tableItem.getTableName().equals(dmlTableName)) {
                        continue;
                    }

                    // 关联条件出现在主表查询条件是否为简单字段
                    boolean allFieldsSimple = true;
                    for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                        if (fieldItem.isMethod() || fieldItem.isBinaryOp()) {
                            allFieldsSimple = false;
                            break;
                        }
                    }

                    // 所有查询字段均为简单字段
                    if (allFieldsSimple) {
                        // 不是子查询
                        if (!tableItem.isSubQuery()) {
                            // ------关联表简单字段更新------
                            Map<String, Object> esFieldData = new LinkedHashMap<>();
                            for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                                if (old.containsKey(fieldItem.getColumn().getColumnName())) {
                                    Object value = esTemplate.getValFromData(config.getEsMapping(),
                                        data,
                                        fieldItem.getFieldName(),
                                        fieldItem.getColumn().getColumnName());
                                    esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), value);
                                }
                            }
                            joinTableSimpleFieldOperation(config, dml, data, tableItem, esFieldData);
                        } else {
                            // ------关联子表简单字段更新------
                            subTableSimpleFieldOperation(config, dml, data, old, tableItem);
                        }
                    } else {
                        // ------关联子表复杂字段更新 执行全sql更新es------
                        wholeSqlOperation(config, dml, data, old, tableItem);
                    }
                }
            }

            i++;
        }
    }

    /**
     * 删除操作dml
     *
     * @param config es配置
     * @param dml dml数据
     */
    private void delete(ESSyncConfig config, Dml dml) {
        List<Map<String, Object>> dataList = dml.getData();
        if (dataList == null || dataList.isEmpty()) {
            return;
        }
        SchemaItem schemaItem = config.getEsMapping().getSchemaItem();

        for (Map<String, Object> data : dataList) {
            if (data == null || data.isEmpty()) {
                continue;
            }

            ESMapping mapping = config.getEsMapping();

            // ------是主表------
            if (schemaItem.getMainTable().getTableName().equalsIgnoreCase(dml.getTable())) {
                if (mapping.get_id() != null) {
                    FieldItem idFieldItem = schemaItem.getIdFieldItem(mapping);
                    // 主键为简单字段
                    if (!idFieldItem.isMethod() && !idFieldItem.isBinaryOp()) {
                        Object idVal = esTemplate.getValFromData(mapping,
                            data,
                            idFieldItem.getFieldName(),
                            idFieldItem.getColumn().getColumnName());

                        if (logger.isTraceEnabled()) {
                            logger.trace("Main table delete es index, destination:{}, table: {}, index: {}, id: {}",
                                config.getDestination(),
                                dml.getTable(),
                                mapping.get_index(),
                                idVal);
                        }
                        esTemplate.delete(mapping, idVal, null);
                    } else {
                        // ------主键带函数, 查询sql获取主键删除------
                        // FIXME 删除时反查sql为空记录, 无法获获取 id field 值
                        mainTableDelete(config, dml, data);
                    }
                } else {
                    FieldItem pkFieldItem = schemaItem.getIdFieldItem(mapping);
                    if (!pkFieldItem.isMethod() && !pkFieldItem.isBinaryOp()) {
                        Map<String, Object> esFieldData = new LinkedHashMap<>();
                        Object pkVal = esTemplate.getESDataFromDmlData(mapping, data, esFieldData);

                        if (logger.isTraceEnabled()) {
                            logger.trace("Main table delete es index, destination:{}, table: {}, index: {}, pk: {}",
                                config.getDestination(),
                                dml.getTable(),
                                mapping.get_index(),
                                pkVal);
                        }
                        esFieldData.remove(pkFieldItem.getFieldName());
                        esFieldData.keySet().forEach(key -> esFieldData.put(key, null));
                        esTemplate.delete(mapping, pkVal, esFieldData);
                    } else {
                        // ------主键带函数, 查询sql获取主键删除------
                        mainTableDelete(config, dml, data);
                    }
                }

            } else {
                // 对象字段涉及的子表数据删除
                updateObjectFields(config, dml, data, null);
            }

            // 从表的操作
            for (TableItem tableItem : schemaItem.getAliasTableItems().values()) {
                if (tableItem.isMain()) {
                    continue;
                }
                if (!tableItem.getTableName().equals(dml.getTable())) {
                    continue;
                }

                // 关联条件出现在主表查询条件是否为简单字段
                boolean allFieldsSimple = true;
                for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                    if (fieldItem.isMethod() || fieldItem.isBinaryOp()) {
                        allFieldsSimple = false;
                        break;
                    }
                }

                // 所有查询字段均为简单字段
                if (allFieldsSimple) {
                    // 不是子查询
                    if (!tableItem.isSubQuery()) {
                        // ------关联表简单字段更新为null------
                        Map<String, Object> esFieldData = new LinkedHashMap<>();
                        for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                            esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), null);
                        }
                        joinTableSimpleFieldOperation(config, dml, data, tableItem, esFieldData);
                    } else {
                        // ------关联子表简单字段更新------
                        subTableSimpleFieldOperation(config, dml, data, null, tableItem);
                    }
                } else {
                    // ------关联子表复杂字段更新 执行全sql更新es------
                    wholeSqlOperation(config, dml, data, null, tableItem);
                }
            }
        }
    }

    /**
     * 单表简单字段insert
     *
     * @param config es配置
     * @param dml dml信息
     * @param data 单行dml数据
     */
    private void singleTableSimpleFiledInsert(ESSyncConfig config, Dml dml, Map<String, Object> data) {

        ESMapping mapping = config.getEsMapping();

        Map<String, Object> esFieldData = new LinkedHashMap<>();
        Object idVal = esTemplate.getESDataFromDmlData(mapping, data, esFieldData);

        //填充对象字段值
        esFieldData.putAll(getObjectFieldDatasForMainTableInsert(config, esFieldData));

        if (logger.isTraceEnabled()) {
            logger.trace("Single table insert to es index, destination:{}, table: {}, index: {}, id: {}",
                config.getDestination(),
                dml.getTable(),
                mapping.get_index(),
                idVal);
        }
        esTemplate.insert(mapping, idVal, esFieldData);
    }

    /**
     * 主表(单表)复杂字段insert
     *
     * @param config es配置
     * @param dml dml信息
     * @param data 单行dml数据
     */
    private void mainTableInsert(ESSyncConfig config, Dml dml, Map<String, Object> data) {
        ESMapping mapping = config.getEsMapping();
        String sql = mapping.getSql();
        String condition = ESSyncUtil.pkConditionSql(mapping, data);
        sql = ESSyncUtil.appendCondition(sql, condition);
        DataSource ds = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        if (logger.isTraceEnabled()) {
            logger.trace("Main table insert to es index by query sql, destination:{}, table: {}, index: {}, sql: {}",
                config.getDestination(),
                dml.getTable(),
                mapping.get_index(),
                sql.replace("\n", " "));
        }
        Util.sqlRS(ds, sql, rs -> {
            try {
                while (rs.next()) {
                    Map<String, Object> esFieldData = new LinkedHashMap<>();
                    Object idVal = esTemplate.getESDataFromRS(mapping, rs, esFieldData);

                    //填充对象字段值
                    esFieldData.putAll(getObjectFieldDatasForMainTableInsert(config, esFieldData));

                    if (logger.isTraceEnabled()) {
                        logger.trace(
                            "Main table insert to es index by query sql, destination:{}, table: {}, index: {}, id: {}",
                            config.getDestination(),
                            dml.getTable(),
                            mapping.get_index(),
                            idVal);
                    }
                    esTemplate.insert(mapping, idVal, esFieldData);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return 0;
        });
    }

    private void mainTableDelete(ESSyncConfig config, Dml dml, Map<String, Object> data) {
        ESMapping mapping = config.getEsMapping();
        String sql = mapping.getSql();
        String condition = ESSyncUtil.pkConditionSql(mapping, data);
        sql = ESSyncUtil.appendCondition(sql, condition);
        DataSource ds = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        if (logger.isTraceEnabled()) {
            logger.trace("Main table delete es index by query sql, destination:{}, table: {}, index: {}, sql: {}",
                config.getDestination(),
                dml.getTable(),
                mapping.get_index(),
                sql.replace("\n", " "));
        }
        Util.sqlRS(ds, sql, rs -> {
            try {
                Map<String, Object> esFieldData = null;
                if (mapping.getPk() != null) {
                    esFieldData = new LinkedHashMap<>();
                    esTemplate.getESDataFromDmlData(mapping, data, esFieldData);
                    esFieldData.remove(mapping.getPk());
                    for (String key : esFieldData.keySet()) {
                        esFieldData.put(Util.cleanColumn(key), null);
                    }
                }
                while (rs.next()) {
                    Object idVal = esTemplate.getIdValFromRS(mapping, rs);

                    if (logger.isTraceEnabled()) {
                        logger.trace(
                            "Main table delete to es index by query sql, destination:{}, table: {}, index: {}, id: {}",
                            config.getDestination(),
                            dml.getTable(),
                            mapping.get_index(),
                            idVal);
                    }
                    esTemplate.delete(mapping, idVal, esFieldData);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return 0;
        });
    }

    /**
     * 关联表主表简单字段operation
     *
     * @param config es配置
     * @param dml dml信息
     * @param data 单行dml数据
     * @param tableItem 当前表配置
     */
    private void joinTableSimpleFieldOperation(ESSyncConfig config, Dml dml, Map<String, Object> data,
                                               TableItem tableItem, Map<String, Object> esFieldData) {
        ESMapping mapping = config.getEsMapping();

        Map<String, Object> paramsTmp = new LinkedHashMap<>();
        for (Map.Entry<FieldItem, List<FieldItem>> entry : tableItem.getRelationTableFields().entrySet()) {
            for (FieldItem fieldItem : entry.getValue()) {
                if (fieldItem.getColumnItems().size() == 1) {
                    Object value = esTemplate.getValFromData(mapping,
                        data,
                        fieldItem.getFieldName(),
                        entry.getKey().getColumn().getColumnName());

                    String fieldName = fieldItem.getFieldName();
                    // 判断是否是主键
                    if (fieldName.equals(mapping.get_id())) {
                        fieldName = "_id";
                    }
                    paramsTmp.put(fieldName, value);
                }
            }
        }

        if (logger.isDebugEnabled()) {
            logger.trace("Join table update es index by foreign key, destination:{}, table: {}, index: {}",
                config.getDestination(),
                dml.getTable(),
                mapping.get_index());
        }
        esTemplate.updateByQuery(config, paramsTmp, esFieldData);
    }

    /**
     * 关联子查询, 主表简单字段operation
     *
     * @param config es配置
     * @param dml dml信息
     * @param data 单行dml数据
     * @param old 单行old数据
     * @param tableItem 当前表配置
     */
    private void subTableSimpleFieldOperation(ESSyncConfig config, Dml dml, Map<String, Object> data,
                                              Map<String, Object> old, TableItem tableItem) {
        ESMapping mapping = config.getEsMapping();

        MySqlSelectQueryBlock queryBlock = SqlParser.parseSQLSelectQueryBlock(tableItem.getSubQuerySql());
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ")
            .append(SqlParser.parse4SQLSelectItem(queryBlock))
            .append(" FROM ")
            .append(SqlParser.parse4FromTableSource(queryBlock));

        String whereSql = SqlParser.parse4WhereItem(queryBlock);
        if (whereSql != null) {
            sql.append(" WHERE ").append(whereSql);
        } else {
            sql.append(" WHERE 1=1 ");
        }

        List<Object> values = new ArrayList<>();

        for (FieldItem fkFieldItem : tableItem.getRelationTableFields().keySet()) {
            String columnName = fkFieldItem.getColumn().getColumnName();
            Object value = esTemplate.getValFromData(mapping, data, fkFieldItem.getFieldName(), columnName);
            sql.append(" AND ").append(columnName).append("=? ");
            values.add(value);
        }

        String groupSql = SqlParser.parse4GroupBy(queryBlock);
        if (groupSql != null) {
            sql.append(groupSql);
        }

        DataSource ds = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        if (logger.isTraceEnabled()) {
            logger.trace("Join table update es index by query sql, destination:{}, table: {}, index: {}, sql: {}",
                config.getDestination(),
                dml.getTable(),
                mapping.get_index(),
                sql.toString().replace("\n", " "));
        }
        Util.sqlRS(ds, sql.toString(), values, rs -> {
            try {
                while (rs.next()) {
                    Map<String, Object> esFieldData = new LinkedHashMap<>();

                    for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                        if (old != null) {
                            out: for (FieldItem fieldItem1 : tableItem.getSubQueryFields()) {
                                for (ColumnItem columnItem0 : fieldItem.getColumnItems()) {
                                    if (fieldItem1.getFieldName().equals(columnItem0.getColumnName()))
                                        for (ColumnItem columnItem : fieldItem1.getColumnItems()) {
                                            if (old.containsKey(columnItem.getColumnName())) {
                                                Object val = esTemplate.getValFromRS(mapping,
                                                    rs,
                                                    fieldItem.getFieldName(),
                                                    fieldItem.getColumn().getColumnName());
                                                esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), val);
                                                break out;
                                            }
                                        }
                                }
                            }
                        } else {
                            Object val = esTemplate.getValFromRS(mapping,
                                rs,
                                fieldItem.getFieldName(),
                                fieldItem.getColumn().getColumnName());
                            esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), val);
                        }
                    }

                    Map<String, Object> paramsTmp = new LinkedHashMap<>();
                    for (Map.Entry<FieldItem, List<FieldItem>> entry : tableItem.getRelationTableFields().entrySet()) {
                        for (FieldItem fieldItem : entry.getValue()) {
                            if (fieldItem.getColumnItems().size() == 1) {
                                Object value = esTemplate.getValFromRS(mapping,
                                    rs,
                                    fieldItem.getFieldName(),
                                    entry.getKey().getColumn().getColumnName());
                                String fieldName = fieldItem.getFieldName();
                                // 判断是否是主键
                                if (fieldName.equals(mapping.get_id())) {
                                    fieldName = "_id";
                                }
                                paramsTmp.put(fieldName, value);
                            }
                        }
                    }

                    if (logger.isDebugEnabled()) {
                        logger.trace("Join table update es index by query sql, destination:{}, table: {}, index: {}",
                            config.getDestination(),
                            dml.getTable(),
                            mapping.get_index());
                    }
                    esTemplate.updateByQuery(config, paramsTmp, esFieldData);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return 0;
        });
    }

    /**
     * 关联(子查询), 主表复杂字段operation, 全sql执行
     *
     * @param config es配置
     * @param dml dml信息
     * @param data 单行dml数据
     * @param tableItem 当前表配置
     */
    private void wholeSqlOperation(ESSyncConfig config, Dml dml, Map<String, Object> data, Map<String, Object> old,
                                   TableItem tableItem) {
        ESMapping mapping = config.getEsMapping();
        // 防止最后出现groupby 导致sql解析异常
        String[] sqlSplit = mapping.getSql().split("GROUP\\ BY(?!(.*)ON)");
        String sqlNoWhere = sqlSplit[0];

        String sqlGroupBy = "";

        if (sqlSplit.length > 1) {
            sqlGroupBy = "GROUP BY " + sqlSplit[1];
        }

        StringBuilder sql = new StringBuilder(sqlNoWhere + " WHERE ");

        for (FieldItem fkFieldItem : tableItem.getRelationTableFields().keySet()) {
            String columnName = fkFieldItem.getColumn().getColumnName();
            Object value = esTemplate.getValFromData(mapping, data, fkFieldItem.getFieldName(), columnName);
            ESSyncUtil.appendCondition(sql, value, tableItem.getAlias(), columnName);
        }
        int len = sql.length();
        sql.delete(len - 5, len);
        sql.append(sqlGroupBy);

        DataSource ds = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        if (logger.isTraceEnabled()) {
            logger.trace("Join table update es index by query whole sql, destination:{}, table: {}, index: {}, sql: {}",
                config.getDestination(),
                dml.getTable(),
                mapping.get_index(),
                sql.toString().replace("\n", " "));
        }
        Util.sqlRS(ds, sql.toString(), rs -> {
            try {
                while (rs.next()) {
                    Map<String, Object> esFieldData = new LinkedHashMap<>();
                    for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                        if (old != null) {
                            // 从表子查询
                            out: for (FieldItem fieldItem1 : tableItem.getSubQueryFields()) {
                                for (ColumnItem columnItem0 : fieldItem.getColumnItems()) {
                                    if (fieldItem1.getFieldName().equals(columnItem0.getColumnName()))
                                        for (ColumnItem columnItem : fieldItem1.getColumnItems()) {
                                            if (old.containsKey(columnItem.getColumnName())) {
                                                Object val = esTemplate.getValFromRS(mapping,
                                                    rs,
                                                    fieldItem.getFieldName(),
                                                    fieldItem.getFieldName());
                                                esFieldData.put(fieldItem.getFieldName(), val);
                                                break out;
                                            }
                                        }
                                }
                            }
                            // 从表非子查询
                            for (FieldItem fieldItem1 : tableItem.getRelationSelectFieldItems()) {
                                if (fieldItem1.equals(fieldItem)) {
                                    for (ColumnItem columnItem : fieldItem1.getColumnItems()) {
                                        if (old.containsKey(columnItem.getColumnName())) {
                                            Object val = esTemplate.getValFromRS(mapping,
                                                rs,
                                                fieldItem.getFieldName(),
                                                fieldItem.getFieldName());
                                            esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), val);
                                            break;
                                        }
                                    }
                                }
                            }
                        } else {
                            Object val = esTemplate
                                .getValFromRS(mapping, rs, fieldItem.getFieldName(), fieldItem.getFieldName());
                            esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), val);
                        }
                    }

                    Map<String, Object> paramsTmp = new LinkedHashMap<>();
                    for (Map.Entry<FieldItem, List<FieldItem>> entry : tableItem.getRelationTableFields().entrySet()) {
                        for (FieldItem fieldItem : entry.getValue()) {
                            Object value = esTemplate
                                .getValFromRS(mapping, rs, fieldItem.getFieldName(), fieldItem.getFieldName());
                            String fieldName = fieldItem.getFieldName();
                            // 判断是否是主键
                            if (fieldName.equals(mapping.get_id())) {
                                fieldName = "_id";
                            }
                            paramsTmp.put(fieldName, value);
                        }
                    }

                    if (logger.isDebugEnabled()) {
                        logger.trace(
                            "Join table update es index by query whole sql, destination:{}, table: {}, index: {}",
                            config.getDestination(),
                            dml.getTable(),
                            mapping.get_index());
                    }
                    esTemplate.updateByQuery(config, paramsTmp, esFieldData);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return 0;
        });
    }

    /**
     * 单表简单字段update
     *
     * @param config es配置
     * @param dml dml信息
     * @param data 单行data数据
     * @param old 单行old数据
     */
    private void singleTableSimpleFiledUpdate(ESSyncConfig config, Dml dml, Map<String, Object> data,
                                              Map<String, Object> old) {
        ESMapping mapping = config.getEsMapping();
        Map<String, Object> esFieldData = new LinkedHashMap<>();

        Object idVal = esTemplate.getESDataFromDmlData(mapping, data, old, esFieldData);

        if (logger.isTraceEnabled()) {
            logger.trace("Main table update to es index, destination:{}, table: {}, index: {}, id: {}",
                config.getDestination(),
                dml.getTable(),
                mapping.get_index(),
                idVal);
        }
        esTemplate.update(mapping, idVal, esFieldData);
    }

    /**
     * 主表(单表)复杂字段update
     *
     * @param config es配置
     * @param dml dml信息
     * @param data 单行dml数据
     */
    private void mainTableUpdate(ESSyncConfig config, Dml dml, Map<String, Object> data, Map<String, Object> old) {
        ESMapping mapping = config.getEsMapping();
        String sql = mapping.getSql();
        String condition = ESSyncUtil.pkConditionSql(mapping, data);
        sql = ESSyncUtil.appendCondition(sql, condition);
        DataSource ds = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        if (logger.isTraceEnabled()) {
            logger.trace("Main table update to es index by query sql, destination:{}, table: {}, index: {}, sql: {}",
                config.getDestination(),
                dml.getTable(),
                mapping.get_index(),
                sql.replace("\n", " "));
        }
        Util.sqlRS(ds, sql, rs -> {
            try {
                while (rs.next()) {
                    Map<String, Object> esFieldData = new LinkedHashMap<>();
                    Object idVal = esTemplate.getESDataFromRS(mapping, rs, old, esFieldData);

                    if (logger.isTraceEnabled()) {
                        logger.trace(
                            "Main table update to es index by query sql, destination:{}, table: {}, index: {}, id: {}",
                            config.getDestination(),
                            dml.getTable(),
                            mapping.get_index(),
                            idVal);
                    }
                    esTemplate.update(mapping, idVal, esFieldData);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return 0;
        });
    }

    /**
     * 批量获取对象字段值(适用：子表 增删改)，并把子表sql中有关主表的查询条件信息放入conditions4Main
     * @param config            es配置
     * @param dml               dml信息
     * @param data              单行dml数据
     * @param oldData           被改动的字段旧值
     * @param conditions4Main   主数据查询条件
     *
     * @return map（key是对象字段名，value是对象字段值）
     */
    public Map<String, Object> getObjectFieldDatasForChildTableUpdate(
            ESSyncConfig config, Dml dml, Map<String, Object> data, Map<String, Object> oldData, Map<String, Object> conditions4Main) {

        ESMapping esMapping = config.getEsMapping();
        Map<String, ObjField> objFields = esMapping.getObjFields().getFields();
        if (CollectionUtils.isEmpty(objFields)) {
            return Collections.emptyMap();
        }

        Map<String, Object> objectFieldDatas = new LinkedHashMap<>();

        //子表名
        String tableName = dml.getTable();

        objFields.forEach((objFieldName, objField) -> {

            SchemaItem schemaItem = objField.getSchemaItem();

            //只处理 开启了反向更新、跟该表有关的对象字段
            if (!(null != schemaItem
                    && objField.isReverseUpdate()
                    && tableName.equalsIgnoreCase(schemaItem.getMainTable().getTableName()))) {
                return;
            }


            //当是update类型时，需要进行对比是否更新了关键字段，如果没有更新是不需要重新解析对象字段
            if (!CollectionUtils.isEmpty(oldData)) {
                Set<String> oldDataColumnNames = oldData.keySet().stream().filter(StringUtils::isNotBlank).map(String::toUpperCase).collect(Collectors.toSet());
                boolean needUpd =
                        schemaItem.getSelectFields()
                                .values()
                                .stream()
                                .map(FieldItem::getColumnItems)
                                .filter(Objects::nonNull)
                                .flatMap(Collection::stream)
                                .map(ColumnItem::getColumnName)
                                .filter(StringUtils::isNotBlank)
                                .map(String::toUpperCase)
                                .anyMatch(oldDataColumnNames::contains);

                if (!needUpd) {
                    return;
                }
            }

            //主表字段值map（key是主表字段引用信息，value是字段值）
            Map<FieldItem, Optional<Object>> mainFieldValueMap4Each =
                    schemaItem.getAliasTableItems()
                            .get(SqlParser.OBJ_FIELD_SQL_MAIN_TABLE_LOGIC_NAME)
                            .getRelationFields()
                            .stream()
                            .collect(
                                    Collectors.toMap(
                                            RelationFieldsPair::getRightFieldItem,
                                            relationFieldsPair -> {
                                                FieldItem mainFieldItem = relationFieldsPair.getRightFieldItem();
                                                FieldItem childFieldItem = relationFieldsPair.getLeftFieldItem();
                                                //子表字段名
                                                String childFieldName = childFieldItem.getFieldName();
                                                //子表字段值
                                                Object fieldValue = data.get(childFieldName);

                                                //记录所有有关主表的查询条件信息
                                                conditions4Main.put(mainFieldItem.getFieldName(), fieldValue);

                                                //可能为null, Map.merge会报错, 使用Optional规避
                                                return Optional.ofNullable(fieldValue);
                                            },
                                            (v1, v2) -> v2,
                                            LinkedHashMap::new
                                    )
                            );


            Object objFieldValue = getObjectFieldData(config, objFieldName, objField, mainFieldValueMap4Each);
            putObjFieldValue(objectFieldDatas, objFieldName, objFieldValue, objField.getType());
        });

        return objectFieldDatas;
    }

    /**
     * 批量获取对象字段值(适用：主表新增数据)
     * @param config        es配置
     * @param esFieldData   es数据
     * @return map（key是对象字段名，value是对象字段值）
     */
    public Map<String, Object> getObjectFieldDatasForMainTableInsert(ESSyncConfig config, Map<String, Object> esFieldData) {

        long startTs = System.currentTimeMillis();

        ObjFields objFieldsConfig = config.getEsMapping().getObjFields();
        //对象字段
        Map<String, ObjField> objFields = objFieldsConfig.getFields();
        if (CollectionUtils.isEmpty(objFields)) {
            return Collections.emptyMap();
        }

        List<Map.Entry<String, ObjField>> objFieldList =
                objFields.entrySet()
                        .stream()
                        //无sql的直接忽略
                        .filter(entry -> null != entry.getValue().getSchemaItem())
                        .collect(Collectors.toList());

        if (CollectionUtils.isEmpty(objFieldList)) {
            return Collections.emptyMap();
        }

        //串行查询对象字段值
        if (objFieldList.size() <=1 || objFieldsConfig.getThreadSize() <= 1 || !objFieldsConfig.isParallel()) {
            Map<String, Object> objectFieldDatas = new LinkedHashMap<>();
            for (Map.Entry<String, ObjField> entry : objFieldList) {
                getObjectFieldDatasForMainTableInsert(config, esFieldData, entry.getKey(), entry.getValue(), objectFieldDatas);
            }
            return objectFieldDatas;
        }

        //并行查询对象字段值, 无法保证字段顺序
        ExecutorService executor = Util.newFixedThreadPool(objFieldsConfig.getThreadSize(), 5000L);
        List<Future<?>> futures = new ArrayList<>();

        //不能用ConcurrentHashMap, 因为他不允许值为null
        List<Map<String, Object>> objectFieldDatas = Collections.synchronizedList(new ArrayList<>());

        try {

            for (Map.Entry<String, ObjField> entry : objFieldList) {

                futures.add(executor.submit(() -> {
                    Map<String, Object> map = new LinkedHashMap<>();
                    objectFieldDatas.add(map);

                    getObjectFieldDatasForMainTableInsert(config, esFieldData, entry.getKey(), entry.getValue(), map);
                }));
            }

            futures.forEach(future -> {
                try {
                    future.get();
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

        } finally {
            executor.shutdown();
        }

        if (logger.isTraceEnabled()) {
            logger.trace("getObjectFieldDatasForMainTableInsert - cost:{}; esFieldData={}", System.currentTimeMillis()-startTs, JSON.toJSONString(esFieldData));
        }

        //value可能为null
        Map<String, Object> map = new LinkedHashMap<>();
        objectFieldDatas.stream().map(Map::entrySet).flatMap(Collection::stream).forEach(entry -> map.put(entry.getKey(), entry.getValue()));
        return map;
    }

    /**
     * 获取对象字段值(适用：主表新增数据), 并把对象字段值放入objectFieldDatas然后返回
     * @param config            es配置
     * @param esFieldData       es数据
     * @param objFieldName      对象字段名
     * @param objField          对象字段
     * @param objectFieldDatas  对象字段值map（key是对象字段名，value是对象字段值）, 为null时会新实例化
     * @return map（key是对象字段名，value是对象字段值）
     */
    private void getObjectFieldDatasForMainTableInsert(
            ESSyncConfig config, Map<String, Object> esFieldData,
            String objFieldName, ObjField objField, Map<String, Object> objectFieldDatas) {
        //主表字段值map（key是主表字段引用信息，value是字段值）
        Map<FieldItem, Optional<Object>> mainFieldValueMap =
                objField.getSchemaItem()
                        .getAliasTableItems()
                        .get(SqlParser.OBJ_FIELD_SQL_MAIN_TABLE_LOGIC_NAME)
                        .getRelationFields()
                        .stream()
                        .map(RelationFieldsPair::getRightFieldItem)
                        .collect(
                                Collectors.toMap(
                                        Function.identity(),
                                        //可能为null, Map.merge会报错, 使用Optional规避
                                        mainField -> Optional.ofNullable(esFieldData.get(mainField.getFieldName())),
                                        (v1, v2) -> v2,
                                        LinkedHashMap::new
                                )
                        );

        Object objFieldValue = getObjectFieldData(config, objFieldName, objField, mainFieldValueMap);
        putObjFieldValue(objectFieldDatas, objFieldName, objFieldValue, objField.getType());
    }

    /**
     * 获取对象字段值
     * @param config                es配置
     * @param objFieldName          对象字段名
     * @param objField              对象字段配置
     * @param mainFieldValueMap     主表字段值map（key是主表字段引用信息，value是字段值）
     * @return 对象字段值
     */
    private Object getObjectFieldData(
            ESSyncConfig config, String objFieldName, ObjField objField,
            Map<FieldItem, Optional<Object>> mainFieldValueMap) {

        SchemaItem schemaItem = objField.getSchemaItem();

        //分隔符
        String separator = objField.getSeparator();
        //对象字段类型
        ObjFieldType objFieldType = objField.getType();
        //子表sql
        String sql = objField.getSql();

        //对象类型，只查询一条
        if (ObjFieldType.object.equals(objFieldType) && StringUtils.isBlank(schemaItem.getLimit())) {
            sql += " limit 1";
        }

        //查询条件值
        List<Object> values = new ArrayList<>();
        //将sql中的主表字段引用表达式替换为占位符
        for (Map.Entry<FieldItem, Optional<Object>> entry : mainFieldValueMap.entrySet()) {
            FieldItem fieldItem = entry.getKey();
            sql = sql.replace(fieldItem.getExpr(), "?");
            values.add(entry.getValue().orElse(null));
        }

        DataSource ds = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        return
                Util.sqlRS(ds, sql, values, rs -> {
                    try {
                        List<Object> list = new ArrayList<>();
                        Supplier<Object> supplier = () -> list;

                        if (ObjFieldType.object.equals(objFieldType) || ObjFieldType.objectFlat.equals(objFieldType)) {

                            //对象类型，只取第一个对象
                            supplier = () -> list.isEmpty() ? null : list.get(0);

                        } else if (ObjFieldType.joining.equals(objFieldType)) {

                            supplier = () -> list.isEmpty() ? "" : StringUtils.removeEnd(list.get(0).toString(), separator);

                        }

                        while (rs.next()) {

                            ResultSetMetaData metaData = rs.getMetaData();
                            //字段数
                            int columnCount = metaData.getColumnCount();

                            if (ObjFieldType.array.equals(objFieldType)) {
                                //数组 (只取第一个字段值)

                                list.add(getFieldValueInObjectField(rs, metaData, 1, config, objFieldName, objField));

                            } else if (ObjFieldType.object.equals(objFieldType)
                                        || ObjFieldType.objectArray.equals(objFieldType)
                                        || ObjFieldType.objectFlat.equals(objFieldType)) {
                                //JSON对象、JSON对象数组、JSON对象扁平化

                                JSONObject jsonObject = new JSONObject(true);
                                for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
                                    //字段名
                                    String columnName = metaData.getColumnLabel(columnIndex);

                                    //字段值
                                    Object columnValue = getFieldValueInObjectField(rs, metaData, columnIndex, config, objFieldName, objField);

                                    jsonObject.put(columnName, columnValue);
                                }
                                list.add(jsonObject);

                                if (ObjFieldType.object.equals(objFieldType) || ObjFieldType.objectFlat.equals(objFieldType)) {
                                    //对象类型，只取第一个对象，即可退出结果集遍历
                                    break;
                                }

                            } else if (ObjFieldType.joining.equals(objFieldType)) {
                                //字符串拼接 (只取第一个字段值)

                                StringBuilder joiningBuilder;
                                if (list.isEmpty()) {
                                    joiningBuilder = new StringBuilder();
                                    list.add(joiningBuilder);
                                } else {
                                    joiningBuilder = (StringBuilder) list.get(0);
                                }
                                joiningBuilder.append(getFieldValueInObjectField(rs, metaData, 1, config, objFieldName, objField))
                                        .append(separator);
                            }
                        }

                        return supplier.get();

                    } catch (RuntimeException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /**
     * 将对象字段值放入 objectFieldDatas
     * @param objectFieldDatas  对象字段值map
     * @param objFieldName      对象字段名
     * @param objFieldValue     对象字段值
     * @param objFieldType      对象字段类型
     */
    @SuppressWarnings("unchecked")
    private void putObjFieldValue(Map<String, Object> objectFieldDatas, String objFieldName, Object objFieldValue, ObjFieldType objFieldType) {
        if (ObjFieldType.objectFlat.equals(objFieldType)) {
            if (null != objFieldValue) {
                //扁平化放入
                objectFieldDatas.putAll((Map<String, Object>) objFieldValue);
            }
        } else {
            objectFieldDatas.put(objFieldName, objFieldValue);
        }
    }

    /**
     * 获取对象字段内的字段值
     * @param rs                结果集
     * @param metaData          结果集元数据
     * @param columnIndex       字段下标
     * @param config            es配置
     * @param objFieldName      对象字段名
     * @param objField          对象字段配置信息
     * @return
     * @throws SQLException
     */
    private Object getFieldValueInObjectField(
            ResultSet rs, ResultSetMetaData metaData, int columnIndex,
            ESSyncConfig config, String objFieldName, ObjField objField) throws SQLException {

        //字段名
        String columnName = metaData.getColumnLabel(columnIndex);

        //字段值
        Object columnValue = rs.getObject(columnName);

        //es类型
        String esType =
                ObjFieldType.objectFlat.equals(objField.getType())
                        ?
                        esTemplate.getEsType(config.getEsMapping(), columnName)
                        :
                        esTemplate.getEsType(config.getEsMapping(), objFieldName, columnName);

        //根据es类型转换值
        return ESSyncUtil.typeConvert(columnValue, esType);
    }

    /**
     * 对象字段涉及的子表更新(增删改)
     * @param config    es配置
     * @param dml       dml信息
     * @param data      单行data数据
     * @param old       单行old数据（insert、delete情况下没有）
     */
    private void updateObjectFields(ESSyncConfig config, Dml dml, Map<String, Object> data, Map<String, Object> old) {
        //子表sql中有关主表的查询条件信息
        Map<String, Object> conditions4Main = new LinkedHashMap<>();

        //先获取对象字段值
        Map<String, Object> esFieldData = getObjectFieldDatasForChildTableUpdate(config, dml, data, old, conditions4Main);

        //反向更新es主表数据
        esTemplate.updateByQuery(config, conditions4Main, esFieldData);
    }

    /**
     * 提交批次
     */
    public void commit() {
        esTemplate.commit();
    }
}
