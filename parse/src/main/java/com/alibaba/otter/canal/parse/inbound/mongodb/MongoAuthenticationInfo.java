package com.alibaba.otter.canal.parse.inbound.mongodb;

import com.mongodb.ConnectionString;

import java.net.InetSocketAddress;
import java.util.List;

public class MongoAuthenticationInfo {

    private final String hosts;

    private final String username;

    private final String password;

    private final String defaultDatabaseName;

    private final ConnectionString connectionString;

    private final static String URL_TEMPLATE = "mongodb://%s:%s@%s/%s";

    public MongoAuthenticationInfo(String hosts, String username, String password){
        this(hosts, username, password, "admin");
    }

    public MongoAuthenticationInfo(String hosts, String username, String password, String defaultDatabaseName){
        this.hosts = hosts;
        this.username = username;
        this.password = password;
        this.defaultDatabaseName = defaultDatabaseName;
        this.connectionString = new ConnectionString(String.format(URL_TEMPLATE, username, password, hosts, defaultDatabaseName));
    }

    public List<String> getHosts() {
        return this.connectionString.getHosts();
    }

    public String getDatabase() {
        return this.connectionString.getDatabase();
    }

    public ConnectionString getConnectionString() {
        return connectionString;
    }

    public InetSocketAddress getPrimaryINetSocketAddresses() {
        String[] hostChunk = getHosts().get(0).split(":");
        return new InetSocketAddress(hostChunk[0], Integer.parseInt(hostChunk[1]));
    }

}
