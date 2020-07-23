package com.alibaba.otter.canal.connector.tcp.config;

public class TCPConstants {

    public static final String ROOT                 = "canal";

    public static final String CANAL_TCP_HOST       = ROOT + "." + "tcp.server.host";
    public static final String CANAL_TCP_ZK_HOSTS   = ROOT + "." + "tcp.zookeeper.hosts";
    public static final String CANAL_TCP_USERNAME   = ROOT + "." + "tcp.username";
    public static final String CANAL_TCP_PASSWORD   = ROOT + "." + "tcp.password";
    public static final String CANAL_TCP_BATCH_SIZE = ROOT + "." + "tcp.batch.size";
}
