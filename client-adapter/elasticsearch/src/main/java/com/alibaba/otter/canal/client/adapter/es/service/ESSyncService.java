package com.alibaba.otter.canal.client.adapter.es.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig.ESMapping;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfigLoader;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem.FieldItem;
import com.alibaba.otter.canal.client.adapter.es.support.ESSyncUtil;
import com.alibaba.otter.canal.client.adapter.es.support.ESTemplate;
import com.alibaba.otter.canal.client.adapter.es.support.Util;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;

public class ESSyncService {

    private static Logger logger = LoggerFactory.getLogger(ESSyncService.class);

    private ESTemplate    esTemplate;

    public ESSyncService(ESTemplate esTemplate){
        this.esTemplate = esTemplate;
    }

    public void sync(Dml dml) {
        long begin = System.currentTimeMillis();
        String database = dml.getDatabase();
        String table = dml.getTable();
        List<ESSyncConfig> esSyncConfigs = ESSyncConfigLoader.getDbTableEsSyncConfig().get(database + "-" + table);
        if (esSyncConfigs != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Destination: {}, database:{}, table:{}, type:{}, effect index count: {}",
                    dml.getDestination(),
                    dml.getDatabase(),
                    dml.getTable(),
                    dml.getType(),
                    esSyncConfigs.size());
                logger.debug(JSON.toJSONString(dml));
            }

            for (ESSyncConfig config : esSyncConfigs) {
                logger.info("Prepared to sync index: {}, destination: {}",
                    config.getEsMapping().get_index(),
                    dml.getDestination());
                this.sync(config, dml);
                logger.info("Sync completed: {}, destination: {}",
                    config.getEsMapping().get_index(),
                    dml.getDestination());
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Sync elapsed time: {}, effect index count：{}, destination: {}",
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
                // update(config, dml);
            } else if (type != null && type.equalsIgnoreCase("DELETE")) {
                // delete(config, dml);
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Sync elapsed time: {},destination: {}, es index: {}",
                    (System.currentTimeMillis() - begin),
                    dml.getDestination(),
                    config.getEsMapping().get_index());
            }
        } catch (Exception e) {
            logger.error("sync error, es index: {}, DML : {}", config.getEsMapping().get_index(), dml);
            logger.error(e.getMessage(), e);
        }
    }

    private void insert(ESSyncConfig config, Dml dml) {
        List<Map<String, Object>> dataList = dml.getData();
        if (dataList == null || dataList.isEmpty()) {
            return;
        }
        ESMapping mapping = config.getEsMapping();
        SchemaItem schemaItem = config.getEsMapping().getSchemaItem();
        for (Map<String, Object> data : dataList) {
            if (data == null || data.isEmpty()) {
                continue;
            }

            // ------是否单表 & 所有字段都为简单字段------
            if (schemaItem.getAliasTableItems().size() == 1 && schemaItem.isAllFieldsSimple()) {
                Map<String, Object> esFieldData = new HashMap<>();

                // ------通过dml对象插入ES------
                // 获取所有字段
                for (FieldItem fieldItem : schemaItem.getSelectFields().values()) {
                    // 如果是主键字段则不插入
                    if (fieldItem.getFieldName().equals(mapping.get_id())) {
                        continue;
                    }
                    Object value = esTemplate.getDataValue(mapping,
                        data,
                        Util.cleanColumn(fieldItem.getColumnItems().iterator().next().getColumnName()));
                    String fieldName = Util.cleanColumn(fieldItem.getFieldName());
                    if (!mapping.getSkips().contains(fieldName)) {
                        esFieldData.put(fieldName, esTemplate.convertType(mapping, fieldName, value));
                    }
                }

                // 取出id值
                Object idVal;
                if (mapping.get_id() != null) {
                    idVal = esTemplate.getDataValue(mapping,
                        data,
                        Util.cleanColumn(
                            schemaItem.getSelectFields().get(mapping.get_id()).getColumn().getColumnName()));
                } else {
                    idVal = esTemplate.getDataValue(mapping,
                        data,
                        Util.cleanColumn(
                            schemaItem.getSelectFields().get(mapping.getPk()).getColumn().getColumnName()));
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Single table insert ot es index, destination:{}, table: {}, index: {}, id: {}",
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
                return; // 单表插入完成返回
            }

            // ------是否主表------
            if (schemaItem.getMainTable().getTableName().equalsIgnoreCase(dml.getTable())) {
                String sql = mapping.getSql();
                String condition = ESSyncUtil.pkConditaionSql(mapping, data);
                sql = ESSyncUtil.appendCondition(sql, condition);
                DataSource ds = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
//                ESSyncUtil.sqlRS(ds, sql, rs -> {
//                    try {
//                        while (rs.next()) {
//                            Map<String, Object> esFieldData = new HashMap<>();
//
//                            Object idVal = ESSyncUtil.persistEsByRs(transportClient, config, rs, mainTable, esFieldData);
//
//                            if (logger.isDebugEnabled()) {
//                                logger.debug("主表insert, 通过执行sql查询相应记录，并insert到Es, id: {}, esFieldValue: {}", idVal, esFieldData);
//                            }
//                            boolean result = ESSyncUtil.persist(transportClient, config, idVal, esFieldData, OpType.INSERT);
//                            if (!result) {
//                                logger.error("主表insert，通过执行sql查询响应记录，并insert到es, es插入失败，table: {}, index: {}", dml.getTable(), config.getEsSyn().getIndex());
//                            }
//                        }
//                    } catch (Exception e) {
//                        throw new RuntimeException(e);
//                    }
//                    return 0;
//                });
            }
        }
    }
}
