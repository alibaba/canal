package com.alibaba.otter.canal.parse.exception;
import com.alibaba.otter.canal.common.CanalException;

public class ServerLogPurgedException extends CanalException {
    public ServerLogPurgedException(String errorCode) {
        super("ServerLogPurged0: " + errorCode);
    }
}
