package com.alibaba.otter.canal.server.exception;

import com.alibaba.otter.canal.common.CanalException;

/**
 * canal 异常定义
 * 
 * @author jianghang 2012-6-15 下午04:57:35
 * @version 1.0.0
 */
public class CanalServerException extends CanalException {

    private static final long serialVersionUID = -7288830284122672209L;

    public CanalServerException(String errorCode){
        super(errorCode);
    }

    public CanalServerException(String errorCode, Throwable cause){
        super(errorCode, cause);
    }

    public CanalServerException(String errorCode, String errorDesc){
        super(errorCode + ":" + errorDesc);
    }

    public CanalServerException(String errorCode, String errorDesc, Throwable cause){
        super(errorCode + ":" + errorDesc, cause);
    }

    public CanalServerException(Throwable cause){
        super(cause);
    }

}
