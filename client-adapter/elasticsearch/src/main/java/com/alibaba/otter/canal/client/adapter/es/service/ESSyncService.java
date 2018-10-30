package com.alibaba.otter.canal.client.adapter.es.service;

import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ESSyncService {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private TransportClient transportClient;

    public ESSyncService(TransportClient transportClient) {
        this.transportClient = transportClient;
    }
}
