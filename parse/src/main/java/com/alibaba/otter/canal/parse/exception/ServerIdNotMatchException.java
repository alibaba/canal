package com.alibaba.otter.canal.parse.exception;

import com.alibaba.otter.canal.common.CanalException;

/**
 * @author chengjin.lyf on 2018/8/8 下午1:07
 * @since 1.0.25
 */
public class ServerIdNotMatchException extends CanalException {

    private static final long serialVersionUID = -6124989280379293953L;

    public ServerIdNotMatchException(String errorCode){
        super(errorCode);
    }

    public ServerIdNotMatchException(String errorCode, Throwable cause){
        super(errorCode, cause);
    }

    public ServerIdNotMatchException(String errorCode, String errorDesc){
        super(errorCode, errorDesc);
    }

    public ServerIdNotMatchException(String errorCode, String errorDesc, Throwable cause){
        super(errorCode, errorDesc, cause);
    }

    public ServerIdNotMatchException(Throwable cause){
        super(cause);
    }
}
