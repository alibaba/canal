package com.alibaba.otter.canal.client.adapter.kudu.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kudu.client.KuduException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.client.adapter.kudu.config.KuduMappingConfig;
import com.alibaba.otter.canal.client.adapter.kudu.support.KuduTemplate;
import com.alibaba.otter.canal.client.adapter.support.Dml;

/**
 * @author liuyadong
 * @description kudu实时同步
 */
public class KuduSyncService {

    private static Logger logger = LoggerFactory.getLogger(KuduSyncService.class);

    private KuduTemplate  kuduTemplate;

    // 源库表字段类型缓存: instance.schema.table -> <columnName, jdbcType>
    // private Map<String, Map<String, Integer>> columnsTypeCache = new
    // ConcurrentHashMap<>();

    public KuduSyncService(KuduTemplate kuduTemplate){
        this.kuduTemplate = kuduTemplate;
    }

    // public Map<String, Map<String, Integer>> getColumnsTypeCache() {
    // return columnsTypeCache;
    // }

    /**
     * 同步事件处理
     *
     * @param config
     * @param dml
     */
    public void sync(KuduMappingConfig config, Dml dml) {
        if (config != null) {
            String type = dml.getType();
            if (type != null && type.equalsIgnoreCase("INSERT")) {
                insert(config, dml);
            } else if (type != null && type.equalsIgnoreCase("UPDATE")) {
                upsert(config, dml);
            } else if (type != null && type.equalsIgnoreCase("DELETE")) {
                delete(config, dml);
            }
            if (logger.isDebugEnabled()) {
                logger.debug("DML: {}", JSON.toJSONString(dml, SerializerFeature.WriteMapNullValue));
            }
        }
    }

    /**
     * 删除事件
     *
     * @param config
     * @param dml
     */
    private void delete(KuduMappingConfig config, Dml dml) {
        KuduMappingConfig.KuduMapping kuduMapping = config.getKuduMapping();
        String configTable = kuduMapping.getTable();
        String configDatabase = kuduMapping.getDatabase();
        String table = dml.getTable();
        String database = dml.getDatabase();
        if (configTable.equals(table) && configDatabase.equals(database)) {
            List<Map<String, Object>> data = dml.getData();
            if (data == null || data.isEmpty()) {
                return;
            }
            // 判定主键映射
            String pkId = "";
            Map<String, String> targetPk = kuduMapping.getTargetPk();
            for (Map.Entry<String, String> entry : targetPk.entrySet()) {
                String mysqlID = entry.getKey().toLowerCase();
                String kuduID = entry.getValue();
                if (kuduID == null) {
                    pkId = mysqlID;
                } else {
                    pkId = kuduID;
                }
            }
            // 切割联合主键
            List<String> pkIds = Arrays.asList(pkId.split(","));
            try {
                int idx = 1;
                boolean completed = false;
                List<Map<String, Object>> dataList = new ArrayList<>();

                for (Map<String, Object> item : data) {
                    Map<String, Object> primaryKeyMap = new HashMap<>();
                    for (Map.Entry<String, Object> entry : item.entrySet()) {
                        String columnName = entry.getKey().toLowerCase();
                        Object value = entry.getValue();
                        if (pkIds.contains(columnName)) {
                            primaryKeyMap.put(columnName, value);
                        }
                    }
                    dataList.add(primaryKeyMap);
                    idx++;
                    if (idx % kuduMapping.getCommitBatch() == 0) {
                        kuduTemplate.delete(kuduMapping.getTargetTable(), dataList);
                        dataList.clear();
                        completed = true;
                    }
                }
                if (!completed) {
                    kuduTemplate.delete(kuduMapping.getTargetTable(), dataList);
                }
            } catch (KuduException e) {
                logger.error(e.getMessage());
                logger.error("DML: {}", JSON.toJSONString(dml, SerializerFeature.WriteMapNullValue));
            }
        }
    }

    /**
     * 更新插入事件
     *
     * @param config
     * @param dml
     */
    private void upsert(KuduMappingConfig config, Dml dml) {
        KuduMappingConfig.KuduMapping kuduMapping = config.getKuduMapping();
        String configTable = kuduMapping.getTable();
        String configDatabase = kuduMapping.getDatabase();
        String table = dml.getTable();
        String database = dml.getDatabase();
        if (configTable.equals(table) && configDatabase.equals(database)) {
            List<Map<String, Object>> data = dml.getData();
            if (data == null || data.isEmpty()) {
                return;
            }
            try {
                int idx = 1;
                boolean completed = false;
                List<Map<String, Object>> dataList = new ArrayList<>();

                for (Map<String, Object> entry : data) {
                    dataList.add(entry);
                    idx++;
                    if (idx % kuduMapping.getCommitBatch() == 0) {
                        kuduTemplate.upsert(kuduMapping.getTargetTable(), dataList);
                        dataList.clear();
                        completed = true;
                    }
                }
                if (!completed) {
                    kuduTemplate.upsert(kuduMapping.getTargetTable(), dataList);
                }
            } catch (KuduException e) {
                logger.error(e.getMessage());
                logger.error("DML: {}", JSON.toJSONString(dml, SerializerFeature.WriteMapNullValue));
            }
        }

    }

    /**
     * 插入事件
     *
     * @param config
     * @param dml
     */
    private void insert(KuduMappingConfig config, Dml dml) {
        KuduMappingConfig.KuduMapping kuduMapping = config.getKuduMapping();
        String configTable = kuduMapping.getTable();
        String configDatabase = kuduMapping.getDatabase();
        String table = dml.getTable();
        String database = dml.getDatabase();
        if (configTable.equals(table) && configDatabase.equals(database)) {
            List<Map<String, Object>> data = dml.getData();
            if (data == null || data.isEmpty()) {
                return;
            }
            try {
                int idx = 1;
                boolean completed = false;
                List<Map<String, Object>> dataList = new ArrayList<>();

                for (Map<String, Object> entry : data) {
                    dataList.add(entry);
                    idx++;
                    if (idx % kuduMapping.getCommitBatch() == 0) {
                        kuduTemplate.insert(kuduMapping.getTargetTable(), dataList);
                        dataList.clear();
                        completed = true;
                    }
                }
                if (!completed) {
                    kuduTemplate.insert(kuduMapping.getTargetTable(), dataList);
                }
            } catch (KuduException e) {
                logger.error(e.getMessage());
                logger.error("DML: {}", JSON.toJSONString(dml, SerializerFeature.WriteMapNullValue));
            }
        }
    }

}
