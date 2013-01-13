package com.alibaba.otter.canal.parse.exception;

import com.alibaba.otter.canal.common.CanalException;

/**
 * canal 异常定义
 * 
 * @author jianghang 2012-6-15 下午04:57:35
 * @version 1.0.0
 */
public class CanalHAException extends CanalException {

    private static final long serialVersionUID = -7288830284122672209L;

    public CanalHAException(String errorCode){
        super(errorCode);
    }

    public CanalHAException(String errorCode, Throwable cause){
        super(errorCode, cause);
    }

    public CanalHAException(String errorCode, String errorDesc){
        super(errorCode + ":" + errorDesc);
    }

    public CanalHAException(String errorCode, String errorDesc, Throwable cause){
        super(errorCode + ":" + errorDesc, cause);
    }

    public CanalHAException(Throwable cause){
        super(cause);
    }

}
