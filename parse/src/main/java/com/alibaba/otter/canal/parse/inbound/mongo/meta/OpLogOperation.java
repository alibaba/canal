package com.alibaba.otter.canal.parse.inbound.mongo.meta;

/**
 * @author dsqin
 * @date 2018/5/9
 */
public enum OpLogOperation {

    NoOp("n"), Insert("i"), Update("u"), Delete("d"), Unknown("!");

    private final char statusCode;

    OpLogOperation(String statusCode) {
        this.statusCode = statusCode.charAt(0);
    }

    public char getStatusCode() {
        return statusCode;
    }

    public static OpLogOperation find(String oplogCode) {
        if (oplogCode == null || oplogCode.length() == 0) {
            return OpLogOperation.Unknown;
        }
        for (OpLogOperation value : OpLogOperation.values()) {
            if (value.getStatusCode() == oplogCode.charAt(0)) {
                return value;
            }
        }
        return OpLogOperation.Unknown;
    }
}
