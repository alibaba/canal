package com.alibaba.otter.canal.parse.inbound.mongo.meta;

import com.google.common.collect.Lists;
import com.mongodb.*;
import org.bson.types.BSONTimestamp;

import java.util.List;

/**
 * mongo oplog读取类
 * @author dsqin
 * @date 2018/5/10
 */
public class MongoOpLogReader {

    private static final String OPLOG = "oplog.rs";
    private static final String LOCAL = "local";
    private static final String TS = "ts";
    private static final int BATCH_SIZE = 50;

    private final MongoClient mongoClient;
    private final DBCollection dbCollection;

    public MongoOpLogReader(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
        DB database = mongoClient.getDB(LOCAL);
        this.dbCollection = database.getCollection(OPLOG);
    }

    public List<OpLogMeta> fetch(long timestamp) {
        BSONTimestamp lastTimestamp = BSONTimestampUtil.decode(timestamp);

        BasicDBObject query = new BasicDBObject();
        query.append(TS, new BasicDBObject(QueryOperators.GT, lastTimestamp));
        DBCursor cursor = dbCollection.find(query).sort(new BasicDBObject("$natural", 1)).batchSize(BATCH_SIZE);

        List<OpLogMeta> opLogMetas = Lists.newArrayList();
        int i = 0;
        try {
            while (cursor != null && cursor.hasNext() && i < BATCH_SIZE) {
                DBObject dbObject = cursor.next();

                lastTimestamp = (BSONTimestamp) dbObject.get(TS);
                OpLogMeta opLogEvent = new OpLogMeta(lastTimestamp, dbObject);
                opLogMetas.add(opLogEvent);
                i++;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        //释放DBCursor
        cursor.close();

        return opLogMetas;
    }

}
