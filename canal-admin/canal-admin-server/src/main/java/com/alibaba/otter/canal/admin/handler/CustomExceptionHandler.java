package com.alibaba.otter.canal.admin.handler;

import com.alibaba.otter.canal.admin.common.exception.ServiceException;
import com.alibaba.otter.canal.admin.model.BaseModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

@ControllerAdvice(annotations = ResponseBody.class)
public class CustomExceptionHandler {

    private static Logger logger = LoggerFactory.getLogger(CustomExceptionHandler.class);

    @ResponseBody
    @ResponseStatus(HttpStatus.OK)
    @ExceptionHandler(value = Exception.class)
    public BaseModel commonExceptionHandle(Exception e) {
        if (e instanceof ServiceException) {
            logger.error(e.getMessage());
        } else {
            logger.error(e.getMessage(), e);
        }
        BaseModel res = new BaseModel();
        res.setCode(50000);
        res.setMessage(e.getMessage());
        return res;
    }
}
