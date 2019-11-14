package com.alibaba.otter.canal.client.adapter.kudu.service;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.client.adapter.kudu.config.KuduMappingConfig;
import com.alibaba.otter.canal.client.adapter.kudu.support.KuduTemplate;
import com.alibaba.otter.canal.client.adapter.kudu.support.SyncUtil;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.Util;
import org.apache.kudu.client.KuduException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ━━━━━━神兽出没━━━━━━
 * 　　　┏┓　　　┏┓
 * 　　┏┛┻━━━┛┻┓
 * 　　┃　　　━　　　┃
 * 　　┃　┳┛　┗┳　┃
 * 　　┃　　　┻　　　┃
 * 　　┗━┓　　　┏━┛
 * 　　　　┃　　　┃  神兽保佑
 * 　　　　┃　　　┃  代码无bug
 * 　　　　┃　　　┗━━━┓
 * 　　　　┃　　　　　　　┣┓
 * 　　　　┃　　　　　　　┏┛
 * 　　　　┗┓┓┏━┳┓┏┛
 * 　　　　　┃┫┫　┃┫┫
 * 　　　　　┗┻┛　┗┻┛
 * ━━━━━━感觉萌萌哒━━━━━━
 * Created by Liuyadong on 2019-11-12
 *
 * @description kudu实时同步
 */
public class KuduSyncService {
    private static Logger logger = LoggerFactory.getLogger(KuduSyncService.class);

    private KuduTemplate kuduTemplate;
    private DruidDataSource dataSource;

    // 源库表字段类型缓存: instance.schema.table -> <columnName, jdbcType>
    private Map<String, Map<String, Integer>> columnsTypeCache = new ConcurrentHashMap<>();

    public KuduSyncService(KuduTemplate kuduTemplate, DruidDataSource dataSource) {
        this.kuduTemplate = kuduTemplate;
        this.dataSource = dataSource;
    }

    public Map<String, Map<String, Integer>> getColumnsTypeCache() {
        return columnsTypeCache;
    }

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
                update(config, dml);
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
        List<Map<String, Object>> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }
        KuduMappingConfig.KuduMapping kuduMapping = config.getKuduMapping();

        //判定主键映射
        String pkId = "";
        Map<String, String> targetPk = kuduMapping.getTargetPk();
        for (Map.Entry<String, String> entry : targetPk.entrySet()) {
            String mysqlID = entry.getKey();
            String kuduID = entry.getValue();
            if (kuduID == null) {
                pkId = mysqlID;
            } else {
                pkId = kuduID;
            }
        }

        try {
            //获取mysql库字段类型
            Map<String, Integer> targetColumnType = getTargetColumnType(dataSource.getConnection(), config);
            int idx = 1;
            boolean completed = false;
            List<Map<String, Object>> dataList = new ArrayList<>();

            for (Map<String, Object> item : data) {
                for (Map.Entry<String, Object> entry : item.entrySet()) {
                    String columnName = entry.getKey();
                    Object value = entry.getValue();
                    if (columnName.equals(pkId)) {
                        Map<String, Object> primaryKeyMap = new HashMap<>();
                        primaryKeyMap.put(columnName, value);
                        dataList.add(primaryKeyMap);
                    }
                }
                idx++;
                if (idx % kuduMapping.getCommitBatch() == 0) {
                    kuduTemplate.delete(kuduMapping.getTargetTable(), dataList, targetColumnType);
                    dataList.clear();
                    completed = true;
                }
            }
            if (!completed) {
                kuduTemplate.delete(kuduMapping.getTargetTable(), dataList, targetColumnType);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    /**
     * 更新事件
     *
     * @param config
     * @param dml
     */
    private void update(KuduMappingConfig config, Dml dml) {
        List<Map<String, Object>> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }
        KuduMappingConfig.KuduMapping kuduMapping = config.getKuduMapping();
        try {
            //获取mysql库字段类型
            Map<String, Integer> targetColumnType = getTargetColumnType(dataSource.getConnection(), config);
            int idx = 1;
            boolean completed = false;
            List<Map<String, Object>> dataList = new ArrayList<>();

            for (Map<String, Object> entry : data) {
                dataList.add(entry);
                idx++;
                if (idx % kuduMapping.getCommitBatch() == 0) {
                    kuduTemplate.update(kuduMapping.getTargetTable(), dataList, targetColumnType);
                    dataList.clear();
                    completed = true;
                }
            }
            if (!completed) {
                kuduTemplate.update(kuduMapping.getTargetTable(), dataList, targetColumnType);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    /**
     * 插入事件
     *
     * @param config
     * @param dml
     */
    private void insert(KuduMappingConfig config, Dml dml) {
        List<Map<String, Object>> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }
        KuduMappingConfig.KuduMapping kuduMapping = config.getKuduMapping();
        try {
            //获取mysql库字段类型
            Map<String, Integer> targetColumnType = getTargetColumnType(dataSource.getConnection(), config);
            int idx = 1;
            boolean completed = false;
            List<Map<String, Object>> dataList = new ArrayList<>();

            for (Map<String, Object> entry : data) {
                dataList.add(entry);
                idx++;
                if (idx % kuduMapping.getCommitBatch() == 0) {
                    kuduTemplate.insert(kuduMapping.getTargetTable(), dataList, targetColumnType);
                    dataList.clear();
                    completed = true;
                }
            }
            if (!completed) {
                kuduTemplate.insert(kuduMapping.getTargetTable(), dataList, targetColumnType);
            }
        } catch (SQLException e) {
            logger.error(e.getMessage());
        } catch (KuduException e) {
            logger.error(e.getMessage());
        }

    }


    /**
     * 获取目标字段类型
     *
     * @param conn   sql connection
     * @param config 映射配置
     * @return 字段sqlType
     */
    private Map<String, Integer> getTargetColumnType(Connection conn, KuduMappingConfig config) {
        KuduMappingConfig.KuduMapping dbMapping = config.getKuduMapping();
        String cacheKey = config.getDestination() + "." + dbMapping.getDatabase() + "." + dbMapping.getTable();
        Map<String, Integer> columnType = columnsTypeCache.get(cacheKey);
        if (columnType == null) {
            synchronized (KuduSyncService.class) {
                columnType = columnsTypeCache.get(cacheKey);
                if (columnType == null) {
                    columnType = new LinkedHashMap<>();
                    final Map<String, Integer> columnTypeTmp = columnType;
                    String sql = "SELECT * FROM " + SyncUtil.getDbTableName(dbMapping) + " WHERE 1=2";
                    Util.sqlRS(conn, sql, rs -> {
                        try {
                            ResultSetMetaData rsd = rs.getMetaData();
                            int columnCount = rsd.getColumnCount();
                            for (int i = 1; i <= columnCount; i++) {
                                columnTypeTmp.put(rsd.getColumnName(i).toLowerCase(), rsd.getColumnType(i));
                            }
                            columnsTypeCache.put(cacheKey, columnTypeTmp);
                        } catch (SQLException e) {
                            logger.error(e.getMessage(), e);
                        }
                    });
                }
            }
        }
        return columnType;
    }
}
