package com.alibaba.otter.canal.parse.inbound;

/**
 * @author chengjin.lyf on 2018/7/20 下午3:55
 * @since 1.0.25
 */
public interface ParserExceptionHandler {

    void handle(Throwable e);
}
