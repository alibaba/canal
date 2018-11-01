package com.alibaba.otter.canal.client.adapter.es.service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig.ESMapping;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfigLoader;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem.FieldItem;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem.TableItem;
import com.alibaba.otter.canal.client.adapter.es.support.ESSyncUtil;
import com.alibaba.otter.canal.client.adapter.es.support.ESTemplate;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;

public class ESSyncService {

    private static Logger logger = LoggerFactory.getLogger(ESSyncService.class);

    private ESTemplate    esTemplate;

    public ESSyncService(ESTemplate esTemplate){
        this.esTemplate = esTemplate;
    }

    public void sync(Dml dml) {
        if (logger.isDebugEnabled()) {
            logger.debug("DML: {}", JSON.toJSONString(dml));
        }
        long begin = System.currentTimeMillis();
        String database = dml.getDatabase();
        String table = dml.getTable();
        List<ESSyncConfig> esSyncConfigs = ESSyncConfigLoader.getDbTableEsSyncConfig().get(database + "-" + table);
        if (esSyncConfigs != null) {
            if (logger.isTraceEnabled()) {
                logger.trace("Destination: {}, database:{}, table:{}, type:{}, effect index count: {}",
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
                logger.trace("Sync elapsed time: {} ms, effect index count：{}, destination: {}",
                    (System.currentTimeMillis() - begin),
                    esSyncConfigs.size(),
                    dml.getDestination());
            }
        }
    }

    public void sync(ESSyncConfig config, Dml dml) {
        try {
            long begin = System.currentTimeMillis();

            String type = dml.getType();
            if (type != null && type.equalsIgnoreCase("INSERT")) {
                insert(config, dml);
            } else if (type != null && type.equalsIgnoreCase("UPDATE")) {
                update(config, dml);
            } else if (type != null && type.equalsIgnoreCase("DELETE")) {
                delete(config, dml);
            }

            if (logger.isTraceEnabled()) {
                logger.trace("Sync elapsed time: {} ms,destination: {}, es index: {}",
                    (System.currentTimeMillis() - begin),
                    dml.getDestination(),
                    config.getEsMapping().get_index());
            }
        } catch (Exception e) {
            logger.error("sync error, es index: {}, DML : {}", config.getEsMapping().get_index(), dml);
            logger.error(e.getMessage(), e);
        }
    }

    private void delete(ESSyncConfig config, Dml dml) {

    }

    private void update(ESSyncConfig config, Dml dml) {

    }

    /**
     * 插入dml操作
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
        for (Map<String, Object> data : dataList) {
            if (data == null || data.isEmpty()) {
                continue;
            }

            // ------是否单表 & 所有字段都为简单字段------
            if (schemaItem.getAliasTableItems().size() == 1 && schemaItem.isAllFieldsSimple()) {
                singleTableSimpleFiledInsert(config, dml, data);
            } else {
                // ------是主表 查询sql来插入------
                if (schemaItem.getMainTable().getTableName().equalsIgnoreCase(dml.getTable())) {
                    mainTableInsert(config, dml, data);
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
                            // ------关联表简单字段插入------
                            joinTableSimpleFieldOperation(config, dml, data, tableItem);
                        } else {
                            // ------关联子表简单字段插入------
                            subTableSimpleFieldOperation(config, dml, data, tableItem);
                        }
                    } else {
                        // ------关联子表复杂字段插入 执行全sql更新es------
                        jonTableWholeSqlOperation(config, dml, data, tableItem);
                    }
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

        if (logger.isTraceEnabled()) {
            logger.trace("Single table insert ot es index, destination:{}, table: {}, index: {}, id: {}",
                config.getDestination(),
                dml.getTable(),
                mapping.get_index(),
                idVal);
        }
        boolean result = esTemplate.insert(config, idVal, esFieldData);
        if (!result) {
            logger.error("Single table insert to es index error, destination:{}, table: {}, index: {}, id: {}",
                config.getDestination(),
                dml.getTable(),
                mapping.get_index(),
                idVal);
        }
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
            logger.trace("Single table insert ot es index by query sql, destination:{}, table: {}, index: {}, sql: {}",
                config.getDestination(),
                dml.getTable(),
                mapping.get_index(),
                sql.replace("\n", " "));
        }
        ESSyncUtil.sqlRS(ds, sql, rs -> {
            try {
                while (rs.next()) {
                    Map<String, Object> esFieldData = new LinkedHashMap<>();
                    Object idVal = esTemplate.getESDataFromRS(mapping, rs, esFieldData);

                    if (logger.isTraceEnabled()) {
                        logger.trace(
                            "Single table insert ot es index by query sql, destination:{}, table: {}, index: {}, id: {}",
                            config.getDestination(),
                            dml.getTable(),
                            mapping.get_index(),
                            idVal);
                    }
                    boolean result = esTemplate.insert(config, idVal, esFieldData);
                    if (!result) {
                        logger.error(
                            "Single table insert to es index by query sql error, destination:{}, table: {}, index: {}, id: {}",
                            config.getDestination(),
                            dml.getTable(),
                            mapping.get_index(),
                            idVal);
                    }
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
                                               TableItem tableItem) {
        ESMapping mapping = config.getEsMapping();
        Map<String, Object> esFieldData = new LinkedHashMap<>();
        for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
            Object value = esTemplate
                .getValFromData(mapping, data, fieldItem.getFieldName(), fieldItem.getColumn().getColumnName());
            esFieldData.put(fieldItem.getFieldName(), value);
        }

        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
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
                    queryBuilder.must(QueryBuilders.termsQuery(fieldName, value));
                }
            }
        }

        if (logger.isDebugEnabled()) {
            logger.trace("Join table update es index by foreign key, destination:{}, table: {}, index: {}",
                config.getDestination(),
                dml.getTable(),
                mapping.get_index());
        }
        boolean result = esTemplate.updateByQuery(mapping, queryBuilder, esFieldData);
        if (!result) {
            logger.error("Join table update es index by foreign key error, destination:{}, table: {}, index: {}",
                config.getDestination(),
                dml.getTable(),
                mapping.get_index());
        }
    }

    /**
     * 关联子查询, 主表简单字段operation
     *
     * @param config es配置
     * @param dml dml信息
     * @param data 单行dml数据
     * @param tableItem 当前表配置
     */
    private void subTableSimpleFieldOperation(ESSyncConfig config, Dml dml, Map<String, Object> data,
                                              TableItem tableItem) {
        ESMapping mapping = config.getEsMapping();
        StringBuilder sql = new StringBuilder(
            "SELECT * FROM (" + tableItem.getSubQuerySql() + ") " + tableItem.getAlias() + " WHERE ");

        for (FieldItem fkFieldItem : tableItem.getRelationTableFields().keySet()) {
            String columnName = fkFieldItem.getColumn().getColumnName();
            Object value = esTemplate.getValFromData(mapping, data, fkFieldItem.getFieldName(), columnName);
            if (value instanceof String) {
                sql.append(tableItem.getAlias())
                    .append(".")
                    .append(columnName)
                    .append("='")
                    .append(value)
                    .append("' ")
                    .append(" AND ");
            } else {
                sql.append(tableItem.getAlias())
                    .append(".")
                    .append(columnName)
                    .append("=")
                    .append(value)
                    .append(" ")
                    .append(" AND ");
            }
        }
        int len = sql.length();
        sql.delete(len - 5, len);
        DataSource ds = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        if (logger.isTraceEnabled()) {
            logger.trace("Join table update es index by query sql, destination:{}, table: {}, index: {}, sql: {}",
                config.getDestination(),
                dml.getTable(),
                mapping.get_index(),
                sql.toString().replace("\n", " "));
        }
        ESSyncUtil.sqlRS(ds, sql.toString(), rs -> {
            try {
                while (rs.next()) {
                    Map<String, Object> esFieldData = new LinkedHashMap<>();
                    for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                        Object val = esTemplate
                            .getValFromRS(mapping, rs, fieldItem.getFieldName(), fieldItem.getColumn().getColumnName());
                        esFieldData.put(fieldItem.getFieldName(), val);
                    }

                    BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
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
                                queryBuilder.must(QueryBuilders.termsQuery(fieldName, value));
                            }
                        }
                    }

                    if (logger.isDebugEnabled()) {
                        logger.trace("Join table update es index by query sql, destination:{}, table: {}, index: {}",
                            config.getDestination(),
                            dml.getTable(),
                            mapping.get_index());
                    }
                    boolean result = esTemplate.updateByQuery(mapping, queryBuilder, esFieldData);
                    if (!result) {
                        logger.error(
                            "Join table update es index by query sql error, destination:{}, table: {}, index: {}",
                            config.getDestination(),
                            dml.getTable(),
                            mapping.get_index());
                    }
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
    private void jonTableWholeSqlOperation(ESSyncConfig config, Dml dml, Map<String, Object> data,
                                           TableItem tableItem) {
        ESMapping mapping = config.getEsMapping();
        StringBuilder sql = new StringBuilder(mapping.getSql() + " WHERE ");

        for (FieldItem fkFieldItem : tableItem.getRelationTableFields().keySet()) {
            String columnName = fkFieldItem.getColumn().getColumnName();
            Object value = esTemplate.getValFromData(mapping, data, fkFieldItem.getFieldName(), columnName);
            if (value instanceof String) {
                sql.append(tableItem.getAlias())
                    .append(".")
                    .append(columnName)
                    .append("='")
                    .append(value)
                    .append("' ")
                    .append(" AND ");
            } else {
                sql.append(tableItem.getAlias())
                    .append(".")
                    .append(columnName)
                    .append("=")
                    .append(value)
                    .append(" ")
                    .append(" AND ");
            }
        }
        int len = sql.length();
        sql.delete(len - 5, len);
        DataSource ds = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        if (logger.isTraceEnabled()) {
            logger.trace("Join table update es index by query whole sql, destination:{}, table: {}, index: {}, sql: {}",
                config.getDestination(),
                dml.getTable(),
                mapping.get_index(),
                sql.toString().replace("\n", " "));
        }
        ESSyncUtil.sqlRS(ds, sql.toString(), rs -> {
            try {
                while (rs.next()) {
                    Map<String, Object> esFieldData = new LinkedHashMap<>();
                    for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                        Object val = esTemplate
                            .getValFromRS(mapping, rs, fieldItem.getFieldName(), fieldItem.getFieldName());
                        esFieldData.put(fieldItem.getFieldName(), val);
                    }

                    BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
                    for (Map.Entry<FieldItem, List<FieldItem>> entry : tableItem.getRelationTableFields().entrySet()) {
                        for (FieldItem fieldItem : entry.getValue()) {
                            if (fieldItem.getColumnItems().size() == 1) {
                                Object value = esTemplate
                                    .getValFromRS(mapping, rs, fieldItem.getFieldName(), fieldItem.getFieldName());
                                String fieldName = fieldItem.getFieldName();
                                // 判断是否是主键
                                if (fieldName.equals(mapping.get_id())) {
                                    fieldName = "_id";
                                }
                                queryBuilder.must(QueryBuilders.termsQuery(fieldName, value));
                            }
                        }
                    }

                    if (logger.isDebugEnabled()) {
                        logger.trace(
                            "Join table update es index by query whole sql, destination:{}, table: {}, index: {}",
                            config.getDestination(),
                            dml.getTable(),
                            mapping.get_index());
                    }
                    boolean result = esTemplate.updateByQuery(mapping, queryBuilder, esFieldData);
                    if (!result) {
                        logger.error(
                            "Join table update es index by query whole sql error, destination:{}, table: {}, index: {}",
                            config.getDestination(),
                            dml.getTable(),
                            mapping.get_index());
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return 0;
        });
    }
}
