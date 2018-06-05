package com.alibaba.otter.canal.parse.inbound.mongo.meta;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.DBObject;
import org.apache.commons.lang.math.NumberUtils;
import org.bson.types.BSONTimestamp;

/**
 * @author dsqin
 * @date 2018/5/9
 */
public class OpLogMeta {

    public final static String UPDATE_CONTENT = "c";
    public final static String UPDATE_WHERE = "w";

    private final OpLogOperation operation;
    private final long timestamp;
    private final String nameSpace;
    private final String data;
    private final long sequence;

    private final OpLogType opLogType;

    public enum OpLogType {
        JSON
    }

    public OpLogMeta(OpLogOperation operation, long timestamp, String nameSpace, String data, long sequence) {
        super();
        this.operation = operation;
        this.timestamp = timestamp;
        this.nameSpace = nameSpace;
        this.data = data;
        opLogType = OpLogType.JSON;
        this.sequence = sequence;
    }

    public OpLogMeta(BSONTimestamp ts, DBObject dbObject) {
        this.timestamp = BSONTimestampUtil.encode(ts);
        this.operation = OpLogOperation.find((String) dbObject.get("op"));
        this.nameSpace = (String) dbObject.get("ns");
        if (OpLogOperation.Update == this.operation) {

            String updateContent = ((DBObject)dbObject.get("o")).toString();
            String updateWhere = ((DBObject)dbObject.get("o2")).toString();
            JSONObject jsonObject = new JSONObject();
            jsonObject.put(UPDATE_CONTENT, updateContent);
            jsonObject.put(UPDATE_WHERE, updateWhere);

            this.data = jsonObject.toJSONString();
        } else {
            this.data = ((DBObject) dbObject.get("o")).toString();
        }
        this.sequence = NumberUtils.toLong(dbObject.get("h").toString(), 0L);
        this.opLogType = OpLogType.JSON;
    }

    public OpLogOperation getOperation() {
        return operation;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getNameSpace() {
        return nameSpace;
    }

    public String getData() {
        return data;
    }

    public OpLogType getOpLogType() {
        return opLogType;
    }

    public long getSequence() {
        return sequence;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("OpLogMeta [operation=");
        builder.append(operation);
        builder.append(", timestamp=");
        builder.append(timestamp);
        builder.append(", timestamp milliseconds=");
        builder.append(timestamp);
        builder.append(", nameSpace=");
        builder.append(nameSpace);
        builder.append(", data=");
        builder.append(data);
        builder.append("]");
        return builder.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((data == null) ? 0 : data.hashCode());
        result = prime * result + ((nameSpace == null) ? 0 : nameSpace.hashCode());
        result = prime * result + ((operation == null) ? 0 : operation.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        OpLogMeta other = (OpLogMeta) obj;
        if (data == null) {
            if (other.data != null)
                return false;
        } else if (!data.equals(other.data))
            return false;
        if (nameSpace == null) {
            if (other.nameSpace != null)
                return false;
        } else if (!nameSpace.equals(other.nameSpace))
            return false;
        if (operation != other.operation)
            return false;
        if (timestamp != other.timestamp) {
            return false;
        }
        return true;
    }
}
