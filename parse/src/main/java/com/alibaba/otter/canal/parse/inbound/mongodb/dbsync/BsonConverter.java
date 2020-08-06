package com.alibaba.otter.canal.parse.inbound.mongodb.dbsync;

import com.mongodb.MongoClientSettings;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.concurrent.TimeUnit;

public class BsonConverter {

    private final static CodecRegistry registry = MongoClientSettings.getDefaultCodecRegistry();

    private final static Codec<Document> codec = MongoClientSettings.getDefaultCodecRegistry().get(Document.class);

    public static Document convert(BsonDocument bson) {
        if (bson == null) {
            return null;
        }
        return codec.decode(bson.asBsonReader(), DecoderContext.builder().build());
    }

    public static BsonDocument convert(Document document) {
        if (document == null) {
            return null;
        }
        return document.toBsonDocument(BsonDocument.class, registry);
    }

    public static BsonTimestamp convertTime(long timeMillis) {
        return new BsonTimestamp((int) TimeUnit.MILLISECONDS.toSeconds(timeMillis), 1);
    }

    public static long convertTime(BsonTimestamp bsonTimestamp, TimeUnit timeUnit) {
        return timeUnit.convert(bsonTimestamp.getTime(), TimeUnit.SECONDS);
    }

}
