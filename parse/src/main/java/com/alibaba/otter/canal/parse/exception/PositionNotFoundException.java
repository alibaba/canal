package com.alibaba.otter.canal.parse.exception;

/**
 * @author chengjin.lyf on 2018/7/20 下午2:54
 * @since 1.0.25
 */
public class PositionNotFoundException extends CanalParseException {

    private static final long serialVersionUID = -7382448928116244017L;

    public PositionNotFoundException(String errorCode){
        super(errorCode);
    }

    public PositionNotFoundException(String errorCode, Throwable cause){
        super(errorCode, cause);
    }

    public PositionNotFoundException(String errorCode, String errorDesc){
        super(errorCode, errorDesc);
    }

    public PositionNotFoundException(String errorCode, String errorDesc, Throwable cause){
        super(errorCode, errorDesc, cause);
    }

    public PositionNotFoundException(Throwable cause){
        super(cause);
    }
}
