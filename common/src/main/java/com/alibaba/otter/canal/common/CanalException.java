package com.alibaba.otter.canal.common;

import org.apache.commons.lang.exception.NestableRuntimeException;

/**
 * @author jianghang 2012-7-12 上午10:10:31
 * @version 1.0.0
 */
public class CanalException extends NestableRuntimeException {

    private static final long serialVersionUID = -654893533794556357L;

    public CanalException(String errorCode){
        super(errorCode);
    }

    public CanalException(String errorCode, Throwable cause){
        super(errorCode, cause);
    }

    public CanalException(String errorCode, String errorDesc){
        super(errorCode + ":" + errorDesc);
    }

    public CanalException(String errorCode, String errorDesc, Throwable cause){
        super(errorCode + ":" + errorDesc, cause);
    }

    public CanalException(Throwable cause){
        super(cause);
    }

    public Throwable fillInStackTrace() {
        return this;
    }

}
