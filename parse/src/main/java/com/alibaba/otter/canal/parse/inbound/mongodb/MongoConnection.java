package com.alibaba.otter.canal.parse.inbound.mongodb;

import com.alibaba.otter.canal.common.CanalException;
import com.alibaba.otter.canal.parse.inbound.SinkFunction;
import com.alibaba.otter.canal.parse.inbound.mongodb.dbsync.BsonConverter;
import com.alibaba.otter.canal.parse.inbound.mongodb.dbsync.ChangeStreamEvent;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.mongodb.Block;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Field;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.SocketSettings;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Mongo Connection
 *
 * @author jiabao.sun 2020-07-13 15:23:57
 * @version 1.0.0
 */
public class MongoConnection {

    private   static final  Logger                       logger        = LoggerFactory.getLogger(MongoConnection.class);

    private   static final  long                         detectingRetryTimes = 6;

    private                 AtomicBoolean                connected     = new AtomicBoolean(false);

    private   final         MongoAuthenticationInfo      authenticationInfo;
    private                 MongoClient                  mongoClient;

    protected               int                          connTimeout   = 5 * 1000;           // 5秒
    protected               int                          soTimeout     = 60 * 60 * 1000;     // 1小时

    private                 String                       eventRegex;
    private                 String                       eventBlackRegex;

    private                 AtomicLong                   receivedEventCount;

    public MongoConnection(MongoAuthenticationInfo authenticationInfo){
        Assert.notNull(authenticationInfo, "authentication info can not be null;");
        this.authenticationInfo = authenticationInfo;
    }

    public void connect() throws IOException {
        if (connected.compareAndSet(false, true)) {
            try {

                MongoClientSettings settings = MongoClientSettings.builder()
                        .applyConnectionString(authenticationInfo.getConnectionString())
                        .applyToSocketSettings(new Block<SocketSettings.Builder>() {
                            public void apply(SocketSettings.Builder builder) {
                                 builder.connectTimeout(connTimeout, TimeUnit.MILLISECONDS)
                                        .readTimeout(soTimeout, TimeUnit.MILLISECONDS);
                            }
                        }).build();

                this.mongoClient = MongoClients.create(settings);

                ClusterType clusterType;
                boolean detected = false;
                for (int retryTimes = 0; retryTimes < detectingRetryTimes; retryTimes++) {
                    clusterType = this.mongoClient.getClusterDescription().getType();
                    switch (clusterType) {
                        case UNKNOWN:
                            //返回unknown表示集群正在连接中，过1s后重试
                            logger.info("detecting cluster type...");
                            Thread.sleep(500L);
                            break;
                        case REPLICA_SET:    //Change streams are available for replica sets and sharded clusters:
                        case SHARDED:
                            detected = true;
                            break;
                        case STANDALONE:
                        default:
                            throw new IllegalStateException("unsupported cluster type of " + clusterType);
                    }

                    if (detected) {
                        logger.info("mongo client connected");
                        return;
                    } else if (retryTimes >= detectingRetryTimes - 1) {
                        throw new IllegalStateException("detecting cluster type failed");
                    }
                }
            } catch (Exception e) {
                disconnect();
                throw new IOException("connect to mongodb failed", e);
            }
        } else {
            logger.error("the mongo client {} can't be connected twice.", authenticationInfo.getHosts());
        }
    }

    public void reconnect() throws IOException {
        disconnect();
        connect();
    }

    public void disconnect() throws IOException {
        if (connected.compareAndSet(true, false)) {
            try {
                if (mongoClient != null) {
                    mongoClient.close();
                    mongoClient = null;
                }

                logger.info("disConnect mongo client of {}...", authenticationInfo.getHosts());
            } catch (Exception e) {
                throw new IOException("disconnect mongo client failure", e);
            }
        } else {
            logger.info("the mongo client of {} is not connected", authenticationInfo.getHosts());
        }
    }

    public ServerDescription getPrimary() {
        checkConnected();

        ClusterDescription clusterDesc = this.mongoClient.getClusterDescription();
        List<ServerDescription> primaries = ReadPreference.primary().choose(clusterDesc);
        if (CollectionUtils.isEmpty(primaries)) {
            throw new IllegalStateException("get primary failed, no primary server");
        }

        return primaries.get(0);
    }

    public void dump(long timestamp, SinkFunction<ChangeStreamEvent> func) {
        dump(BsonConverter.convertTime(timestamp), func);
    }

    public void dump(BsonTimestamp bsonTimestamp, SinkFunction<ChangeStreamEvent> func) {
        doDump(bsonTimestamp, func);
    }

    private void doDump(BsonTimestamp bsonTimestamp, SinkFunction<ChangeStreamEvent> func) {
        checkConnected();

        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> changeCursor = openChangeStream(bsonTimestamp)) {
            while (changeCursor.hasNext()) {
                //sink 失败，或者running状态变更退出dump
                if (!func.sink(ChangeStreamEvent.from(changeCursor.next()))) {
                    logger.warn("sink failed, close change stream");
                    break;
                }
                accumulateReceivedCount();
            }
        } catch (Throwable e) {
            throw new CanalException("watch change stream failed", e);
        }

    }

    private MongoChangeStreamCursor<ChangeStreamDocument<Document>> openChangeStream(BsonTimestamp startAtOperationTime) {
        List<Bson> pipeline = Lists.newArrayList(Aggregates.addFields(new Field<>("schemeTable"
                        , new Document("$concat", Lists.newArrayList("$ns.db", ".", "$ns.coll")))));

        List<Bson> filters = Lists.newArrayList();
        if (!Strings.isNullOrEmpty(eventRegex)) {
            filters.add(Filters.regex("schemeTable", eventRegex));
        }

        if (!Strings.isNullOrEmpty(eventBlackRegex)) {
            filters.add(Filters.not(Filters.regex("schemeTable", eventBlackRegex)));
        }

        if (!filters.isEmpty()) {
            pipeline.add(Aggregates.match(Filters.and(filters)));
        }

        return mongoClient.watch(pipeline)
                .startAtOperationTime(startAtOperationTime)
                .cursor();
    }

    private void checkConnected() {
        if (!connected.get()) {
            throw new IllegalStateException("operation failed, the mongo client is not connected");
        }
    }

    private void accumulateReceivedCount() {
        if (receivedEventCount != null) {
            receivedEventCount.addAndGet(1);
        }
    }

    public void setEventRegex(String eventRegex) {
        this.eventRegex = eventRegex;
    }

    public void setEventBlackRegex(String eventBlackRegex) {
        this.eventBlackRegex = eventBlackRegex;
    }

    public void setReceivedEventCount(AtomicLong receivedEventCount) {
        this.receivedEventCount = receivedEventCount;
    }

}
