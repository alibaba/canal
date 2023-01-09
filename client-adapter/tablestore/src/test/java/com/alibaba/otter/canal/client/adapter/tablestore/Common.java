package com.alibaba.otter.canal.client.adapter.tablestore;

import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;
import com.alibaba.otter.canal.client.adapter.tablestore.common.PropertyConstants;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Common {

    public static TablestoreAdapter init() {
        //初始化tablestore信息

        DatasourceConfig.DATA_SOURCES.put("defaultDS", TestConstant.dataSource);

        OuterAdapterConfig outerAdapterConfig = new OuterAdapterConfig();
        outerAdapterConfig.setName("tablestore");
        outerAdapterConfig.setKey("tablestore");
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyConstants.TABLESTORE_ACCESSSECRETID, "xxxxxxxxxx");
        properties.put(PropertyConstants.TABLESTORE_ACCESSSECRETKEY, "xxxxxxxxxx");
        properties.put(PropertyConstants.TABLESTORE_ENDPOINT, "https://xxxxxxxxxx");
        properties.put(PropertyConstants.TABLESTORE_INSTANCENAME, "xxxxxxxxxx");
        outerAdapterConfig.setProperties(properties);
        TablestoreAdapter adapter = new TablestoreAdapter();
        Properties prop = getProperties();
        adapter.init(outerAdapterConfig, prop);
        return adapter;
    }

    private static Properties getProperties() {
        File directory = new File("");
        String rootAbsolutePath =directory.getAbsolutePath();
        String filePath = rootAbsolutePath + "\\src/test/resources/tablestore/constant_support_demo.yml";
        Properties prop = new Properties();
        try {
            FileInputStream inputStream = new FileInputStream(filePath);
            prop.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return prop;
    }


}
