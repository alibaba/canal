package com.alibaba.otter.canal.parse.inbound.mysql.tsdb;

/**
 * @author agapple 2017年10月11日 下午8:45:40
 * @since 1.0.25
 */
public class DefaultTableMetaTSDBFactory implements TableMetaTSDBFactory {

    /**
     * 代理一下tableMetaTSDB的获取,使用隔离的spring定义
     */
    public TableMetaTSDB build(String destination, String springXml) {
        return TableMetaTSDBBuilder.build(destination, springXml);
    }

    public void destory(String destination) {
        TableMetaTSDBBuilder.destory(destination);
    }
}
