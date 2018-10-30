package com.alibaba.otter.canal.client.adapter.es;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfigLoader;
import com.alibaba.otter.canal.client.adapter.es.service.ESSyncService;
import com.alibaba.otter.canal.client.adapter.es.support.ESTemplate;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;
import com.alibaba.otter.canal.client.adapter.support.SPI;

/**
 * ES外部适配器
 *
 * @author rewerma 2018-10-20
 * @version 1.0.0
 */
@SPI("es")
public class ESAdapter implements OuterAdapter {

    private static Logger   logger = LoggerFactory.getLogger(ESAdapter.class);

    private TransportClient transportClient;

    private ESSyncService   esSyncService;

    @Override
    public void init(OuterAdapterConfig configuration) {
        try {
            ESSyncConfigLoader.load();

            Map<String, String> properties = configuration.getProperties();
            Settings.Builder settingBuilder = Settings.builder();
            properties.forEach(settingBuilder::put);
            Settings settings = settingBuilder.build();
            transportClient = new PreBuiltTransportClient(settings);
            String[] hostArray = configuration.getHosts().split(",");
            for (String host : hostArray) {
                int i = host.indexOf(":");
                transportClient.addTransportAddress(new TransportAddress(InetAddress.getByName(host.substring(0, i)),
                    Integer.parseInt(host.substring(i + 1))));
            }
            ESTemplate esTemplate = new ESTemplate(transportClient);
            esSyncService = new ESSyncService(esTemplate);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void sync(Dml dml) {
        esSyncService.sync(dml);
    }

    @Override
    public EtlResult etl(String task, List<String> params) {
        return null;
    }

    @Override
    public Map<String, Object> count(String task) {
        return null;
    }

    @Override
    public void destroy() {
        if (transportClient != null) {
            transportClient.close();
        }
    }

    @Override
    public String getDestination(String task) {
        return null;
    }
}
