package com.alibaba.otter.canal.parse.inbound.mysql.tsdb;

/**
 * tableMeta构造器,允许重载实现
 * 
 * @author agapple 2018年8月8日 上午11:01:08
 * @since 1.0.26
 */

public interface TableMetaTSDBFactory {

    /**
     * 代理一下tableMetaTSDB的获取,使用隔离的spring定义
     */
    public TableMetaTSDB build(String destination, String springXml);

    public void destory(String destination);
}
