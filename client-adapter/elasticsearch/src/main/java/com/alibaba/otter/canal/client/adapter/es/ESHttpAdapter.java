package com.alibaba.otter.canal.client.adapter.es;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig.ESMapping;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfigLoader;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem;
import com.alibaba.otter.canal.client.adapter.es.config.SqlParser;
import com.alibaba.otter.canal.client.adapter.es.monitor.ESHttpConfigMonitor;
import com.alibaba.otter.canal.client.adapter.es.service.ESHttpEtlService;
import com.alibaba.otter.canal.client.adapter.es.service.ESHttpSyncService;
import com.alibaba.otter.canal.client.adapter.es.support.ESHttpTemplate;
import com.alibaba.otter.canal.client.adapter.support.*;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.InetAddress;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * ES外部适配器
 *
 * @author rewerma 2018-10-20
 * @version 1.0.0
 */
@SPI("es_http")
public class ESHttpAdapter implements OuterAdapter {
    private static Logger logger = LoggerFactory.getLogger(ESHttpAdapter.class);
    private Map<String, ESSyncConfig>              esSyncConfig        = new ConcurrentHashMap<>(); // 文件名对应配置
    private Map<String, Map<String, ESSyncConfig>> dbTableEsSyncConfig = new ConcurrentHashMap<>(); // schema-table对应配置

    private RestHighLevelClient                    restHighLevelClient;

    private ESHttpSyncService                          esHttpSyncService;

    private ESHttpConfigMonitor                        esConfigMonitor;

    private Properties                             envProperties;

    public RestHighLevelClient getRestHighLevelClient() {
        return restHighLevelClient;
    }

    public ESHttpSyncService getEsHttpSyncService() {
        return esHttpSyncService;
    }

    public Map<String, ESSyncConfig> getEsSyncConfig() {
        return esSyncConfig;
    }

    public Map<String, Map<String, ESSyncConfig>> getDbTableEsSyncConfig() {
        return dbTableEsSyncConfig;
    }

    @Override
    public void init(OuterAdapterConfig configuration, Properties envProperties) {
        try {
            this.envProperties = envProperties;
            envProperties.setProperty("esType", configuration.getName());
            Map<String, ESSyncConfig> esSyncConfigTmp = ESSyncConfigLoader.load(envProperties);
            // 过滤不匹配的key的配置
            esSyncConfigTmp.forEach((key, config) -> {
                if ((config.getOuterAdapterKey() == null && configuration.getKey() == null)
                    || (config.getOuterAdapterKey() != null
                        && config.getOuterAdapterKey().equalsIgnoreCase(configuration.getKey()))) {
                    esSyncConfig.put(key, config);
                }
            });

            for (Map.Entry<String, ESSyncConfig> entry : esSyncConfig.entrySet()) {
                String configName = entry.getKey();
                ESSyncConfig config = entry.getValue();
                SchemaItem schemaItem = SqlParser.parse(config.getEsMapping().getSql());
                config.getEsMapping().setSchemaItem(schemaItem);

                DruidDataSource dataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
                if (dataSource == null || dataSource.getUrl() == null) {
                    throw new RuntimeException("No data source found: " + config.getDataSourceKey());
                }
                Pattern pattern = Pattern.compile(".*:(.*)://.*/(.*)\\?.*$");
                Matcher matcher = pattern.matcher(dataSource.getUrl());
                if (!matcher.find()) {
                    throw new RuntimeException("Not found the schema of jdbc-url: " + config.getDataSourceKey());
                }
                String schema = matcher.group(2);

                schemaItem.getAliasTableItems().values().forEach(tableItem -> {
                    Map<String, ESSyncConfig> esSyncConfigMap;
                    if (envProperties != null
                        && !"tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
                        esSyncConfigMap = dbTableEsSyncConfig
                            .computeIfAbsent(StringUtils.trimToEmpty(config.getDestination()) + "-"
                                             + StringUtils.trimToEmpty(config.getGroupId()) + "_" + schema + "-"
                                             + tableItem.getTableName(),
                                k -> new ConcurrentHashMap<>());
                    } else {
                        esSyncConfigMap = dbTableEsSyncConfig
                            .computeIfAbsent(StringUtils.trimToEmpty(config.getDestination()) + "_" + schema + "-"
                                             + tableItem.getTableName(),
                                k -> new ConcurrentHashMap<>());
                    }

                    esSyncConfigMap.put(configName, config);
                });
            }

            Map<String, String> properties = configuration.getProperties();
            String[] hostArray = configuration.getHosts().split(",");
            HttpHost[] httpHosts = new HttpHost[hostArray.length];
            for (int i = 0; i < hostArray.length;i++) {
                String host = hostArray[i];
                int j = host.indexOf(":");
                HttpHost httpHost = new HttpHost(InetAddress.getByName(host.substring(0, j)),
                        Integer.parseInt(host.substring(j + 1)));
                httpHosts[i] = httpHost;
            }
            RestClientBuilder restClientBuilder = RestClient.builder(httpHosts);
            String nameAndPwd = properties.get("xpack.security.user");
            if (StringUtils.isNotEmpty(nameAndPwd) && nameAndPwd.contains(":")) {
                String[] nameAndPwdArr = nameAndPwd.split(":");
                final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY,
                        new UsernamePasswordCredentials(nameAndPwdArr[0], nameAndPwdArr[1]));
                restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
            }
            restHighLevelClient = new RestHighLevelClient(restClientBuilder);

            ESHttpTemplate esHttpTemplate = new ESHttpTemplate(restHighLevelClient);
            esHttpSyncService = new ESHttpSyncService(esHttpTemplate);

            esConfigMonitor = new ESHttpConfigMonitor();
            esConfigMonitor.init(this, envProperties);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void sync(List<Dml> dmls) {
        if (dmls == null || dmls.isEmpty()) {
            return;
        }
        for (Dml dml : dmls) {
            if (!dml.getIsDdl()) {
                sync(dml);
            }
        }
        esHttpSyncService.commit(); // 批次统一提交

    }

    private void sync(Dml dml) {
        String database = dml.getDatabase();
        String table = dml.getTable();
        Map<String, ESSyncConfig> configMap;
        if (envProperties != null && !"tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
            configMap = dbTableEsSyncConfig
                .get(StringUtils.trimToEmpty(dml.getDestination()) + "-" + StringUtils.trimToEmpty(dml.getGroupId())
                     + "_" + database + "-" + table);
        } else {
            configMap = dbTableEsSyncConfig
                .get(StringUtils.trimToEmpty(dml.getDestination()) + "_" + database + "-" + table);
        }

        if (configMap != null && !configMap.values().isEmpty()) {
            esHttpSyncService.sync(configMap.values(), dml);
        }
    }

    @Override
    public EtlResult etl(String task, List<String> params) {
        EtlResult etlResult = new EtlResult();
        ESSyncConfig config = esSyncConfig.get(task);
        if (config != null) {
            DataSource dataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
            ESHttpEtlService esEtlService = new ESHttpEtlService(restHighLevelClient, config);
            if (dataSource != null) {
                return esEtlService.importData(params);
            } else {
                etlResult.setSucceeded(false);
                etlResult.setErrorMessage("DataSource not found");
                return etlResult;
            }
        } else {
            StringBuilder resultMsg = new StringBuilder();
            boolean resSuccess = true;
            // ds不为空说明传入的是datasourceKey
            for (ESSyncConfig configTmp : esSyncConfig.values()) {
                // 取所有的destination为task的配置
                if (configTmp.getDestination().equals(task)) {
                    ESHttpEtlService esEtlService = new ESHttpEtlService(restHighLevelClient, configTmp);
                    EtlResult etlRes = esEtlService.importData(params);
                    if (!etlRes.getSucceeded()) {
                        resSuccess = false;
                        resultMsg.append(etlRes.getErrorMessage()).append("\n");
                    } else {
                        resultMsg.append(etlRes.getResultMessage()).append("\n");
                    }
                }
            }
            if (resultMsg.length() > 0) {
                etlResult.setSucceeded(resSuccess);
                if (resSuccess) {
                    etlResult.setResultMessage(resultMsg.toString());
                } else {
                    etlResult.setErrorMessage(resultMsg.toString());
                }
                return etlResult;
            }
        }
        etlResult.setSucceeded(false);
        etlResult.setErrorMessage("Task not found");
        return etlResult;
    }

    @Override
    public Map<String, Object> count(String task) {
        ESSyncConfig config = esSyncConfig.get(task);
        ESMapping mapping = config.getEsMapping();
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.size(0);
        SearchRequest request = new SearchRequest(mapping.get_index());
        request.types(mapping.get_type())
                .source(builder);
        SearchResponse response = null;
        try {
            response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            logger.error("在count()函数中执行搜索时出现异常",e);
        }
        long rowCount = -1;
        if (response != null) {
            rowCount = response.getHits().getTotalHits();
        }
        Map<String, Object> res = new LinkedHashMap<>();
        res.put("esIndex", mapping.get_index());
        res.put("count", rowCount);
        return res;
    }

    @Override
    public void destroy() {
        if (esConfigMonitor != null) {
            esConfigMonitor.destroy();
        }
        if (restHighLevelClient != null) {
            try {
                restHighLevelClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public String getDestination(String task) {
        ESSyncConfig config = esSyncConfig.get(task);
        if (config != null) {
            return config.getDestination();
        }
        return null;
    }
}
