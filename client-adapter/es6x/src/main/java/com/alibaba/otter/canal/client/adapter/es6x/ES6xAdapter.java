package com.alibaba.otter.canal.client.adapter.es6x;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.alibaba.otter.canal.client.adapter.es.core.ESAdapter;
import com.alibaba.otter.canal.client.adapter.es.core.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es6x.etl.ESEtlService;
import com.alibaba.otter.canal.client.adapter.es6x.support.ES6xTemplate;
import com.alibaba.otter.canal.client.adapter.es6x.support.ESConnection;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;
import com.alibaba.otter.canal.client.adapter.support.SPI;
import org.elasticsearch.action.search.SearchResponse;

import javax.sql.DataSource;

/**
 * ES 6.x 外部适配器
 *
 * @author rewerma 2019-09-23
 * @version 1.0.0
 */
@SPI("es6")
public class ES6xAdapter extends ESAdapter {

    private ESConnection esConnection;

    public ESConnection getEsConnection() {
        return esConnection;
    }

    @Override
    public void init(OuterAdapterConfig configuration, Properties envProperties) {
        try {
            Map<String, String> properties = configuration.getProperties();

            String[] hostArray = configuration.getHosts().split(",");
            String mode = properties.get("mode");
            if ("rest".equalsIgnoreCase(mode) || "http".equalsIgnoreCase(mode)) {
                esConnection = new ESConnection(hostArray, properties, ESConnection.ESClientMode.REST);
            } else {
                esConnection = new ESConnection(hostArray, properties, ESConnection.ESClientMode.TRANSPORT);
            }
            this.esTemplate = new ES6xTemplate(esConnection);

            envProperties.put("es.version", "es6");
            super.init(configuration, envProperties);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, Object> count(String task) {
        ESSyncConfig config = esSyncConfig.get(task);
        ESSyncConfig.ESMapping mapping = config.getEsMapping();
        SearchResponse response = this.esConnection.new ESSearchRequest(mapping.get_index(), mapping.get_type()).size(0)
            .getResponse();

        long rowCount = response.getHits().getTotalHits();
        Map<String, Object> res = new LinkedHashMap<>();
        res.put("esIndex", mapping.get_index());
        res.put("count", rowCount);
        return res;
    }

    @Override
    public EtlResult etl(String task, List<String> params) {
        EtlResult etlResult = new EtlResult();
        ESSyncConfig config = esSyncConfig.get(task);
        if (config != null) {
            DataSource dataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
            ESEtlService esEtlService = new ESEtlService(esConnection, config);
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
            for (ESSyncConfig configTmp : esSyncConfig.values()) {
                // 取所有的destination为task的配置
                if (configTmp.getDestination().equals(task)) {
                    ESEtlService esEtlService = new ESEtlService(esConnection, configTmp);
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
    public void destroy() {
        super.destroy();
        if (esConnection != null) {
            esConnection.close();
        }
    }
}
