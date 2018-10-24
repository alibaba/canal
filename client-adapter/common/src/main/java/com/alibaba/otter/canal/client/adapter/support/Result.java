package com.alibaba.otter.canal.client.adapter.support;

import java.io.Serializable;
import java.util.Date;

public class Result implements Serializable {

    public Integer code    = 20000;
    public Object  data;
    public String  message;
    public Date    sysTime = new Date();

    public static Result createSuccess(Object data) {
        Result result = new Result();
        result.setData(data);
        return result;
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Date getSysTime() {
        return sysTime;
    }

    public void setSysTime(Date sysTime) {
        this.sysTime = sysTime;
    }
}
