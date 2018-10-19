package com.alibaba.otter.canal.client.adapter.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import com.alibaba.otter.canal.client.adapter.CanalOuterAdapter;
import com.alibaba.otter.canal.client.adapter.hbase.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.hbase.config.MappingConfigLoader;
import com.alibaba.otter.canal.client.adapter.hbase.service.HbaseSyncService;
import com.alibaba.otter.canal.client.adapter.support.CanalOuterAdapterConfiguration;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.SPI;

/**
 * HBase外部适配器
 *
 * @author machengyuan 2018-8-21 下午8:45:38
 * @version 1.0.0
 */
@SPI("hbase")
public class HbaseAdapter implements CanalOuterAdapter {

    private static volatile Map<String, MappingConfig> mappingConfigCache = null;

    private Connection                                 conn;
    private HbaseSyncService                           hbaseSyncService;

    @Override
    public void init(CanalOuterAdapterConfiguration configuration) {
        try {
            if (mappingConfigCache == null) {
                synchronized (MappingConfig.class) {
                    if (mappingConfigCache == null) {
                        Map<String, MappingConfig> hbaseMapping = MappingConfigLoader.load();
                        mappingConfigCache = new HashMap<>();
                        for (MappingConfig mappingConfig : hbaseMapping.values()) {
                            mappingConfigCache.put(mappingConfig.getHbaseOrm().getDatabase() + "-"
                                                   + mappingConfig.getHbaseOrm().getTable(), mappingConfig);
                        }
                    }
                }
            }

            String hosts = configuration.getZkHosts();
            if (StringUtils.isEmpty(hosts)) {
                hosts = configuration.getHosts();
            }
            if (StringUtils.isEmpty(hosts)) {
                throw new RuntimeException("Empty zookeeper hosts");
            }
            String[] zkHosts = StringUtils.split(hosts, ",");
            int zkPort = 0;
            StringBuilder hostsWithoutPort = new StringBuilder();
            for (String host : zkHosts) {
                int i = host.indexOf(":");
                hostsWithoutPort.append(host, 0, i);
                hostsWithoutPort.append(",");
                if (zkPort == 0) zkPort = Integer.parseInt(host.substring(i + 1));
            }
            hostsWithoutPort.deleteCharAt(hostsWithoutPort.length() - 1);

            String znode = configuration.getProperties().getProperty("znodeParent");
            if (StringUtils.isEmpty(znode)) {
                znode = "/hbase";
            }

            Configuration hbaseConfig = HBaseConfiguration.create();
            hbaseConfig.set("hbase.zookeeper.quorum", hostsWithoutPort.toString());
            hbaseConfig.set("hbase.zookeeper.property.clientPort", Integer.toString(zkPort));
            hbaseConfig.set("zookeeper.znode.parent", znode);
            conn = ConnectionFactory.createConnection(hbaseConfig);
            hbaseSyncService = new HbaseSyncService(conn);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeOut(Dml dml) {
        if (dml == null) {
            return;
        }
        String database = dml.getDatabase();
        String table = dml.getTable();
        MappingConfig config = mappingConfigCache.get(database + "-" + table);
        hbaseSyncService.sync(config, dml);
    }

    @Override
    public void destroy() {
        if (conn != null) {
            try {
                conn.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
