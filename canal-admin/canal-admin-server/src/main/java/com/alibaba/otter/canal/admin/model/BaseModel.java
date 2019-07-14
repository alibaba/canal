package com.alibaba.otter.canal.admin.model;

public class BaseModel<T> {

    private Integer code = 20000;
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

    public static <T> BaseModel<T> getInstance(T data) {
        BaseModel<T> baseModel = new BaseModel<>();
        baseModel.data = data;
        return baseModel;
    }
}
