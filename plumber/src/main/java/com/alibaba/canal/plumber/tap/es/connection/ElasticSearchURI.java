package com.alibaba.canal.plumber.tap.es.connection;

import com.google.common.collect.Lists;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;

/**
 * @author dsqin
 * @date 2018/6/5
 */
public class ElasticSearchURI {
    private static final String SCHEMA_ES = "es";

    public static ElasticSearchConnectionParam parse(String uriString)
            throws URISyntaxException
    {
        URI uri = new URI(uriString);

        String schema = uri.getScheme();
        if (!"es".equals(schema)) {
            return null;
        }
        String authority = uri.getAuthority();
        if (StringUtils.isBlank(authority)) {
            return null;
        }
        String[] authoritys = StringUtils.splitByWholeSeparatorPreserveAllTokens(authority, "@");
        if ((ArrayUtils.isEmpty(authoritys)) || (authoritys.length < 2)) {
            return null;
        }
        String ipPortsString = authoritys[0];
        String clusterName = authoritys[1];

        String[] ipPorts = StringUtils.splitByWholeSeparatorPreserveAllTokens(ipPortsString, "?");
        if (ArrayUtils.isEmpty(ipPorts)) {
            return null;
        }
        int ipPortSize = ipPorts.length;

        List<TransportAddress> transportAddresses = Lists.newArrayList();
        for (int i = 0; i < ipPortSize; i++)
        {
            String ipPort = ipPorts[i];

            String[] ipPortArray = StringUtils.splitByWholeSeparatorPreserveAllTokens(ipPort, ":");
            if ((!ArrayUtils.isEmpty(ipPortArray)) && (ipPortArray.length >= 2))
            {
                String ip = ipPortArray[0];
                int port = NumberUtils.toInt(ipPortArray[1], 9300);
                InetSocketAddress inetSocketAddress = new InetSocketAddress(ip, port);

                transportAddresses.add(new InetSocketTransportAddress(inetSocketAddress));
            }
        }
        ElasticSearchConnectionParam elasticSearchConnectionParam = new ElasticSearchConnectionParam();
        elasticSearchConnectionParam.setTransportAddresses(transportAddresses);
        elasticSearchConnectionParam.setClusterName(clusterName);

        return elasticSearchConnectionParam;
    }
}
