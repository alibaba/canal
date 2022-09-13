package com.alibaba.otter.canal.client.adapter.starrocks.support;

import com.alibaba.fastjson2.JSONObject;
import com.alibaba.otter.canal.client.adapter.starrocks.config.MappingConfig;
import com.starrocks.connector.flink.manager.StarRocksSinkBufferEntity;
import com.starrocks.connector.flink.manager.StarRocksSinkManager;
import com.starrocks.connector.flink.row.sink.StarRocksSinkOptions;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Map;

public class StarrocksTemplate {
    private static final Logger logger  = LoggerFactory.getLogger(StarrocksTemplate.class);
    private String jdbcUrl;
     private String loadUrl;
     private String userName;
     private String passWord;
     private String databaseName;

     private  StarRocksSinkManager starRocksSinkManager;

    public StarrocksTemplate(String jdbcUrl, String loadUrl, String userName, String passWord, String databaseName) {
        this.jdbcUrl = jdbcUrl;
        this.loadUrl = loadUrl;
        this.userName = userName;
        this.passWord = passWord;
        this.databaseName = databaseName;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getLoadUrl() {
        return loadUrl;
    }

    public void setLoadUrl(String loadUrl) {
        this.loadUrl = loadUrl;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassWord() {
        return passWord;
    }

    public void setPassWord(String passWord) {
        this.passWord = passWord;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String delete(Map<String, Object> rowData) {
        JSONObject jsonObject = new JSONObject();
        for (Map.Entry<String, Object> column : rowData.entrySet()) {
            if (column != null  && StringUtils.isNotEmpty(column.getKey())) {
                jsonObject.put(column.getKey(), column.getValue());
            }
        }
        jsonObject.put("__op", "1");
        return jsonObject.toJSONString();
    }

    public String upsert(Map<String, Object> rowData) {
        JSONObject jsonObject = new JSONObject();
        for (Map.Entry<String, Object> column : rowData.entrySet()) {
            if (column != null  && StringUtils.isNotEmpty(column.getKey())) {
                jsonObject.put(column.getKey(), column.getValue());
            }
        }
        return jsonObject.toJSONString();
    }

    public void sink(StarRocksBufferData bufferData) {
        if (bufferData == null || CollectionUtils.isEmpty(bufferData.getData())) {
            return;
        }

        MappingConfig.SrMapping srMapping = bufferData.getMappingConfig().getSrMapping();
        String database = srMapping.getDatabase();
        String table = srMapping.getTable();
        String srTable = srMapping.getSrTable();
        logger.info("Sync table {}.{}", database, table);
        StarRocksSinkOptions op = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", "jdbc:mysql://" + this.jdbcUrl)
                .withProperty("load-url", this.loadUrl)
                .withProperty("username", this.userName)
                .withProperty("password",  this.passWord)
                .withProperty("table-name", srTable)
                .withProperty("database-name", this.databaseName)
                .withProperty("sink.properties.format", "json")
                .withProperty("sink.properties.strip_outer_array", "true")
                .withProperty("sink.properties.max_filter_ratio", "0.2")
                .build();

        if (starRocksSinkManager == null) {
            starRocksSinkManager = new StarRocksSinkManager(op, null);
        } else {
            starRocksSinkManager.getStarrocksStreamLoadVisitor().setSinkOptions(op);
        }

        StarRocksSinkBufferEntity bufferEntity = new StarRocksSinkBufferEntity(op.getDatabaseName(), op.getTableName(), null);
        bufferEntity.setBuffer((ArrayList<byte[]>) bufferData.getData());
        for (byte[] bts : bufferData.getData()) {
            bufferEntity.incBatchSize(bts.length);
        }

        try {
            if (bufferData.getData().size() != 0 && bufferEntity.getBatchSize() > 0) {
                logger.info(String.format("StarRocks buffer Sinking triggered: db: [%s] table: [%s] rows[%d] label[%s].", database, table, bufferData.getData().size(), bufferEntity.getLabel()));
                starRocksSinkManager.getStarrocksStreamLoadVisitor().doStreamLoad(bufferEntity);
            }
        }catch (Exception e) {
            logger.error("Sink " + database + "." + table + " data to StarRocks failed. Error message: " + e.getMessage());
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
