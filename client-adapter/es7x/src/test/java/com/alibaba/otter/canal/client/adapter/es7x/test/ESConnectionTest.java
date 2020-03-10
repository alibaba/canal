package com.alibaba.otter.canal.client.adapter.es7x.test;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.util.Assert;

import com.alibaba.otter.canal.client.adapter.es7x.support.ESConnection;

@Ignore
public class ESConnectionTest {

    ESConnection esConnection;

    @Before
    public void init() throws UnknownHostException {
        String[] hosts = new String[] { "127.0.0.1:9200" };
        Map<String, String> properties = new HashMap<>();
        properties.put("cluster.name", "elasticsearch");
        esConnection = new ESConnection(hosts, properties, ESConnection.ESClientMode.REST);
    }

    @Test
    public void test01() {
        MappingMetaData mappingMetaData = esConnection.getMapping("mytest_user");

        Map<String, Object> sourceMap = mappingMetaData.getSourceAsMap();
        Map<String, Object> esMapping = (Map<String, Object>) sourceMap.get("properties");
        for (Map.Entry<String, Object> entry : esMapping.entrySet()) {
            Map<String, Object> value = (Map<String, Object>) entry.getValue();
            if (value.containsKey("properties")) {
                System.out.println(entry.getKey() + " object");
            } else {
                System.out.println(entry.getKey() + " " + value.get("type"));
                Assert.notNull(entry.getKey(), "null column name");
                Assert.notNull(value.get("type"), "null column type");
            }
        }
    }
}
