package com.alibaba.otter.canal.client.adapter.tablestore;


import com.alibaba.otter.canal.client.adapter.support.FileName2KeyMapping;
import com.alibaba.otter.canal.client.adapter.support.Util;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;
import com.alibaba.otter.canal.client.adapter.support.SPI;
import com.alibaba.otter.canal.client.adapter.tablestore.common.PropertyConstants;
import com.alibaba.otter.canal.client.adapter.tablestore.config.ConfigLoader;
import com.alibaba.otter.canal.client.adapter.tablestore.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.tablestore.service.TablestoreEtlService;
import com.alibaba.otter.canal.client.adapter.tablestore.service.TablestoreSyncService;
import com.alicloud.openservices.tablestore.DefaultTableStoreWriter;
import com.alicloud.openservices.tablestore.TableStoreWriter;
import com.alicloud.openservices.tablestore.core.auth.DefaultCredentials;
import com.alicloud.openservices.tablestore.core.auth.ServiceCredentials;
import com.alicloud.openservices.tablestore.writer.WriterConfig;
import com.alicloud.openservices.tablestore.writer.WriterResult;
import com.alicloud.openservices.tablestore.writer.enums.BatchRequestType;
import com.alicloud.openservices.tablestore.writer.enums.DispatchMode;
import com.alicloud.openservices.tablestore.writer.enums.WriteMode;
import com.alicloud.openservices.tablestore.writer.enums.WriterRetryStrategy;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;


@SPI("tablestore")
public class TablestoreAdapter implements OuterAdapter {

    private static final Logger logger              = LoggerFactory.getLogger(TablestoreAdapter.class);

    private Map<String, MappingConfig>              tablestoreMapping          = new ConcurrentHashMap<>();                // 文件名对应配置

    private Map<String, Map<String, MappingConfig>> mappingConfigCache  = new ConcurrentHashMap<>();

    private Map<String, Map<String, TableStoreWriter>> writerCache  = new ConcurrentHashMap<>();

    private TablestoreSyncService tablestoreSyncService;

    private Properties                              envProperties;

    private OuterAdapterConfig configuration;


    @Override
    public void init(OuterAdapterConfig configuration, Properties envProperties) {
        this.envProperties = envProperties;
        this.configuration = configuration;
        Map<String, MappingConfig> tablestoreMappingTmp = ConfigLoader.load(envProperties);
        // 过滤不匹配的key的配置
        tablestoreMappingTmp.forEach((key, config) -> {
            addConfig(key, config);
        });

        if (tablestoreMapping.isEmpty()) {
            throw new RuntimeException("No tablestore adapter found for config key: " + configuration.getKey());
        }

        tablestoreSyncService = new TablestoreSyncService();
    }

    /**
     * 根据配置文件获得tablestorewriter的WriterConfig信息
     * @param mappingConfig
     * @return
     */
    private WriterConfig getWriterConfig(MappingConfig mappingConfig) {
        WriterConfig config = new WriterConfig();
        MappingConfig.DbMapping mapping = mappingConfig.getDbMapping();
        config.setMaxBatchRowsCount(mapping.getCommitBatch());
        config.setConcurrency(mappingConfig.getThreads());
        config.setDispatchMode(DispatchMode.HASH_PRIMARY_KEY);
        config.setWriteMode(WriteMode.SEQUENTIAL);
        config.setBatchRequestType(BatchRequestType.BULK_IMPORT);
        config.setBucketCount(mappingConfig.getThreads());
        config.setWriterRetryStrategy(WriterRetryStrategy.CERTAIN_ERROR_CODE_NOT_RETRY);
        config.setAllowDuplicatedRowInBatchRequest(false);
        return config;
    }


    @Override
    public void sync(List<Dml> dmls) {
        if (dmls == null || dmls.isEmpty()) {
            return;
        }

        try {
            Set<TableStoreWriter> writerSet = new HashSet<>();
            List<Future<WriterResult>> futureList = new ArrayList<>();
            for (Dml dml : dmls) {
                String destination = StringUtils.trimToEmpty(dml.getDestination());
                String groupId = StringUtils.trimToEmpty(dml.getGroupId());
                String database = dml.getDatabase();
                String table = dml.getTable();
                String key;
                if (envProperties != null && !"tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
                    key = destination + "-" + groupId + "_" + database + "-" + table;
                } else {
                    key = destination + "_" + database + "-" + table;
                }
                Map<String, MappingConfig> configMap = mappingConfigCache.get(key);
                if (configMap == null) {
                    // 可能有dml中涉及到的表并没有出现在配置中，说明此类dml并不需要同步
                    continue;
                }

                Map<String, TableStoreWriter> writerMap = writerCache.get(key);

                for (Map.Entry<String, MappingConfig> entry : configMap.entrySet()) {
                    TableStoreWriter w = writerMap.get(entry.getKey());
                    // 拿到所有future用于判定失败的记录
                    Future<WriterResult> futureTemp = tablestoreSyncService.sync(entry.getValue(), dml, w);
                    if (futureTemp != null) {
                        writerSet.add(w);
                        futureList.add(futureTemp);
                    }
                }
            }

            if (writerSet.isEmpty()) {
                return;
            }

            writerSet.forEach(e -> e.flush());

            List<WriterResult.RowChangeStatus> totalFailedRows = new ArrayList<>();
            for (Future<WriterResult> future : futureList) {
                try {
                    WriterResult result = future.get();
                    List<WriterResult.RowChangeStatus> failedRows = result.getFailedRows();
                    if (!CollectionUtils.isEmpty(failedRows)) {
                        totalFailedRows.addAll(failedRows);
                    }
                } catch (InterruptedException e) {
                    logger.info("InterruptedException", e);
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }

            if (!CollectionUtils.isEmpty(totalFailedRows)) {
                // 认为有失败的请求
                List<String> msgs = totalFailedRows.stream().map(e -> buildErrorMsgForFailedRowChange(e)).collect(Collectors.toList());
                throw new RuntimeException("Failed rows:" + org.springframework.util.StringUtils.collectionToDelimitedString(msgs, ",", "[", "]"));
            }

        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * 组装失败记录的信息
     * @param rowChangeStatus
     * @return
     */
    public static String buildErrorMsgForFailedRowChange(WriterResult.RowChangeStatus rowChangeStatus) {
        StringBuilder sb = new StringBuilder("{Exception:");
        sb.append(rowChangeStatus.getException().getMessage()).append(",Table:")
        .append(rowChangeStatus.getRowChange().getTableName()).append(",PrimaryKey:")
        .append("{").append(rowChangeStatus.getRowChange().getPrimaryKey().toString())
        .append("}}");
        return sb.toString();
    }


    @Override
    public EtlResult etl(String task, List<String> params) {
        EtlResult etlResult = new EtlResult();
        MappingConfig config = tablestoreMapping.get(task);
        if (config == null) {
            etlResult.setErrorMessage("can not find config for " + task);
            etlResult.setSucceeded(false);
            return etlResult;
        }

        TableStoreWriter writer = null;
        try {
            writer = buildEtlWriter(configuration, config);

            TablestoreEtlService rdbEtlService = new TablestoreEtlService(writer, config);
            rdbEtlService.importData(params);

            etlResult.setSucceeded(true);
            return etlResult;
        } catch (Exception e) {
            logger.error("Error while etl for task " + task, e);
            etlResult.setSucceeded(false);
            etlResult.setErrorMessage(e.getMessage());
            return etlResult;
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    /**
     * 构造批量导入的writer
     * @param configuration
     * @param mappingConfig
     * @return
     */
    private TableStoreWriter buildEtlWriter(OuterAdapterConfig configuration, MappingConfig mappingConfig) {
        Map<String, String> properties = configuration.getProperties();

        ServiceCredentials credentials = new DefaultCredentials(
                properties.get(PropertyConstants.TABLESTORE_ACCESSSECRETID),
                properties.get(PropertyConstants.TABLESTORE_ACCESSSECRETKEY)
        );

        WriterConfig config = getWriterConfig(mappingConfig);
        config.setBucketCount(3);
        config.setAllowDuplicatedRowInBatchRequest(true);
        config.setConcurrency(8);
        config.setWriteMode(WriteMode.PARALLEL);

        TableStoreWriter writer = new DefaultTableStoreWriter(
                properties.get(PropertyConstants.TABLESTORE_ENDPOINT),
                credentials,
                properties.get(PropertyConstants.TABLESTORE_INSTANCENAME),
                mappingConfig.getDbMapping().getTargetTable(),
                config,
                null
        );
        return writer;
    }


    @Override
    public Map<String, Object> count(String task) {
        throw new RuntimeException("count is not supportted in tablestore");
    }


    @Override
    public String getDestination(String task) {
        MappingConfig config = tablestoreMapping.get(task);
        if (config != null) {
            return config.getDestination();
        }
        return null;
    }

    @Override
    public void destroy() {
        if (tablestoreSyncService != null) {
            tablestoreSyncService.close();
        }

        if (writerCache != null) {
            for (Map<String, TableStoreWriter> tmpMap : writerCache.values()) {
                if (tmpMap != null) {
                    for (TableStoreWriter writer : tmpMap.values()) {
                        writer.close();
                    }
                }
            }
        }
    }

    private void addSyncConfigToCache(String configName, MappingConfig mappingConfig) {
        Map<String, String> properties = configuration.getProperties();
        String key;
        if (envProperties != null && !"tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
            key = StringUtils.trimToEmpty(mappingConfig.getDestination()) + "-"
                    + StringUtils.trimToEmpty(mappingConfig.getGroupId()) + "_"
                    + mappingConfig.getDbMapping().getDatabase() + "-" + mappingConfig.getDbMapping().getTable();
        } else {
            key = StringUtils.trimToEmpty(mappingConfig.getDestination()) + "_"
                    + mappingConfig.getDbMapping().getDatabase() + "-" + mappingConfig.getDbMapping().getTable();
        }
        Map<String, MappingConfig> configMap = mappingConfigCache.computeIfAbsent(key,
                k1 -> new ConcurrentHashMap<>());
        configMap.put(configName, mappingConfig);


        // 构建对应的 TableStoreWriter
        ServiceCredentials credentials = new DefaultCredentials(
                properties.get(PropertyConstants.TABLESTORE_ACCESSSECRETID),
                properties.get(PropertyConstants.TABLESTORE_ACCESSSECRETKEY)
        );


        WriterConfig config = getWriterConfig(mappingConfig);

        TableStoreWriter writer = new DefaultTableStoreWriter(
                properties.get(PropertyConstants.TABLESTORE_ENDPOINT),
                credentials,
                properties.get(PropertyConstants.TABLESTORE_INSTANCENAME),
                mappingConfig.getDbMapping().getTargetTable(),
                config,
                null
        );

        Map<String, TableStoreWriter> config2writerMap = writerCache.computeIfAbsent(key,
                k1 -> new ConcurrentHashMap<>());
        config2writerMap.put(configName, writer);
    }

    public boolean addConfig(String fileName, MappingConfig config) {
        if (match(config)) {
            tablestoreMapping.put(fileName, config);
            addSyncConfigToCache(fileName, config);
            FileName2KeyMapping.register(getClass().getAnnotation(SPI.class).value(), fileName,
                    configuration.getKey());
            return true;
        }
        return false;
    }

    public void updateConfig(String fileName, MappingConfig config) {
        if (config.getOuterAdapterKey() != null && !config.getOuterAdapterKey()
                .equals(configuration.getKey())) {
            // 理论上不允许改这个 因为本身就是通过这个关联起Adapter和Config的
            throw new RuntimeException("not allow to change outAdapterKey");
        }
        tablestoreMapping.put(fileName, config);
        addSyncConfigToCache(fileName, config);
    }

    public void deleteConfig(String fileName) {
        tablestoreMapping.remove(fileName);
        for (Map<String, MappingConfig> configMap : mappingConfigCache.values()) {
            if (configMap != null) {
                configMap.remove(fileName);
            }
        }
        FileName2KeyMapping.unregister(getClass().getAnnotation(SPI.class).value(), fileName);
    }

    private boolean match(MappingConfig config) {
        boolean sameMatch = config.getOuterAdapterKey() != null && config.getOuterAdapterKey()
                .equalsIgnoreCase(configuration.getKey());
        boolean prefixMatch = config.getOuterAdapterKey() == null && configuration.getKey()
                .startsWith(StringUtils
                        .join(new String[]{Util.AUTO_GENERATED_PREFIX, config.getDestination(),
                                config.getGroupId()}, '-'));
        return sameMatch || prefixMatch;
    }
}
