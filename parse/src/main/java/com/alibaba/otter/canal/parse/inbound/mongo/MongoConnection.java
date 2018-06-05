package com.alibaba.otter.canal.parse.inbound.mongo;

import com.alibaba.otter.canal.parse.inbound.SinkFunction;
import com.alibaba.otter.canal.parse.inbound.mongo.meta.MongoOpLogReader;
import com.alibaba.otter.canal.parse.inbound.mongo.meta.OpLogMeta;
import com.alibaba.otter.canal.parse.inbound.mongo.support.HaAuthentication;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.List;

/**
 * mongo数据连接串
 * @author dsqin
 * @date 2018/5/15
 */
public class MongoConnection {

    private MongoClient mongoClient;
    private MongoOpLogReader mongoOpLogReader;
    private String uri;

    public void seek(EntryPosition entryPosition, SinkFunction func)
            throws IOException
    {
        Long position = null;
        if (null != entryPosition) {
            position = entryPosition.getPosition();
        } else {
            position = new Long(0L);
        }
        List<OpLogMeta> opLogMetas = this.mongoOpLogReader.fetch(position.longValue());
        if ((null != opLogMetas) && (!opLogMetas.isEmpty())) {
            for (OpLogMeta opLogMeta : opLogMetas) {
                func.sink(opLogMeta);
            }
        }
    }

    public HaAuthentication buildClient()
            throws URISyntaxException, UnknownHostException
    {
        if (null != this.mongoClient)
        {
            this.mongoClient.close();
            this.mongoClient = null;
        }
        List<ServerAddress> serverAddresses = MongoURI.parseURI(this.uri);
        try
        {
            this.mongoClient = new MongoClient(serverAddresses);
        }
        catch (Throwable throwable)
        {
            throwable.printStackTrace();
        }
        this.mongoClient.getConnector();

        this.mongoOpLogReader = new MongoOpLogReader(this.mongoClient);

        return new HaAuthentication(serverAddresses);
    }

    public void destoryClient()
    {
        if (null != this.mongoClient) {
            this.mongoClient.close();
        }
    }

    public String getUri()
    {
        return this.uri;
    }

    public void setUri(String uri)
    {
        this.uri = uri;
    }

    public MongoClient getMongoClient()
    {
        return this.mongoClient;
    }

    public void setMongoClient(MongoClient mongoClient)
    {
        this.mongoClient = mongoClient;
    }

}
