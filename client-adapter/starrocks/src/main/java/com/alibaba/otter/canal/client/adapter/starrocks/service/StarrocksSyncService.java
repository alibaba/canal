package com.alibaba.otter.canal.client.adapter.starrocks.service;

import com.alibaba.otter.canal.client.adapter.starrocks.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.starrocks.support.StarRocksBufferData;
import com.alibaba.otter.canal.client.adapter.starrocks.support.StarrocksTemplate;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * StarRocks同步操作业务
 */
public class StarrocksSyncService {
    private static final Logger logger  = LoggerFactory.getLogger(StarrocksSyncService.class);

    private StarrocksTemplate starrocksTemplate;

    public StarrocksSyncService(StarrocksTemplate starrocksTemplate) {
        this.starrocksTemplate = starrocksTemplate;
    }

    public void sync(Map<String, Map<String, MappingConfig>> mappingConfig, List<Dml> dmls, Properties envProperties) {

        Map<String, StarRocksBufferData> batchData = new HashMap<>();
        for (Dml dml : dmls) {
            if(dml.getIsDdl() != null && dml.getIsDdl() && StringUtils.isNotEmpty(dml.getSql())) {
                logger.info("This is DDL event data, it will be ignored");
            } else {
                // DML
                String destination = StringUtils.trimToEmpty(dml.getDestination());
                String groupId = StringUtils.trimToEmpty(dml.getGroupId());
                String database = dml.getDatabase();
                String table = dml.getTable();
                Map<String, MappingConfig> configMap;
                String key;
                if (envProperties != null && !"tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
                    key = destination + "-" + groupId + "_" + database + "-" + table;
                    configMap = mappingConfig.get(key);
                } else {
                    key = destination + "_" + database + "-" + table;
                    configMap = mappingConfig.get(key);
                }
                if (configMap == null) {
                  continue;
                }

                if (configMap.values().isEmpty()) {
                    continue;
                }

                for (MappingConfig config : configMap.values()) {
                    StarRocksBufferData rocksBufferData = batchData.computeIfAbsent(key, k -> new StarRocksBufferData());
                    rocksBufferData.setDbName(starrocksTemplate.getDatabaseName());
                    rocksBufferData.setMappingConfig(config);
                    List<byte[]> bufferData = rocksBufferData.getData();
                    List<byte[]> bytesData = genBatchData(dml, config);
                    if (CollectionUtils.isEmpty(bufferData)) {
                        bufferData = new ArrayList<>();
                    }
                    bufferData.addAll(bytesData);
                    rocksBufferData.setData(bufferData);
                }
            }
        }

        sync(batchData);
    }


    /**
     * 批量同步
     * @param batchData 批量数据
     */
    private void sync(Map<String, StarRocksBufferData> batchData) {
        for (Map.Entry<String, StarRocksBufferData> bufferDataEntry : batchData.entrySet()) {
            starrocksTemplate.sink(bufferDataEntry.getValue());
        }
    }

    public List<byte[]> genBatchData(Dml dml, MappingConfig config) {
        List<byte[]> batchData = new ArrayList<>() ;
        List<Map<String, Object>> data = dml.getData();
        if (data == null || data.size() == 0) {
            return null;
        }

        String eventType = config.getSrMapping().getEventType();
        String type = dml.getType();
        for (Map<String, Object> rowData : data) {
            String jsonData;
            if (type != null && type.equalsIgnoreCase("INSERT") && eventType.contains("INSERT")) {
                jsonData = starrocksTemplate.upsert(rowData);
            } else if (type != null && type.equalsIgnoreCase("UPDATE") && eventType.contains("UPDATE")) {
                jsonData = starrocksTemplate.upsert(rowData);
            } else if (type != null && type.equalsIgnoreCase("DELETE") && eventType.contains("DELETE")) {
                jsonData = starrocksTemplate.delete(rowData);
            } else {
                logger.warn("Unsupport other event data");
                continue;
            }

            batchData.add(jsonData.getBytes(StandardCharsets.UTF_8));
        }

        return batchData;
    }
}
