package com.alibaba.canal.plumber.tap.es.connection;

import org.elasticsearch.common.transport.TransportAddress;

import java.util.List;

/**
 * @author dsqin
 * @date 2018/6/5
 */
public class ElasticSearchConnectionParam {

    private List<TransportAddress> transportAddresses;
    private String clusterName;

    public List<TransportAddress> getTransportAddresses()
    {
        return this.transportAddresses;
    }

    public void setTransportAddresses(List<TransportAddress> transportAddresses)
    {
        this.transportAddresses = transportAddresses;
    }

    public String getClusterName()
    {
        return this.clusterName;
    }

    public void setClusterName(String clusterName)
    {
        this.clusterName = clusterName;
    }
}
