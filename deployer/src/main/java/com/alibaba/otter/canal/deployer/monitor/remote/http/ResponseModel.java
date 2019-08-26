package com.alibaba.otter.canal.deployer.monitor.remote.http;

/**
 * 响应类
 *
 * @author rewerma 2019-08-26 上午09:40:36
 * @version 1.0.0
 */
public class ResponseModel<T> {

    private Integer code;
    private String  message;
    private T       data;

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}
