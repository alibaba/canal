package com.alibaba.otter.canal.parse.inbound.mysql.tsdb;

import java.util.Map;

import com.alibaba.otter.canal.parse.inbound.TableMeta;
import com.alibaba.otter.canal.protocol.position.EntryPosition;

/**
 * 表结构的时间序列存储
 *
 * @author agapple 2017年7月27日 下午4:06:30
 * @since 1.0.25
 */
public interface TableMetaTSDB {

    /**
     * 初始化
     */
    public boolean init(String destination);

    /**
     * 销毁资源
     */
    public void destory();

    /**
     * 获取当前的表结构
     */
    public TableMeta find(String schema, String table);

    /**
     * 添加ddl到时间序列库中
     */
    public boolean apply(EntryPosition position, String schema, String ddl, String extra);

    /**
     * 回滚到指定位点的表结构
     */
    public boolean rollback(EntryPosition position);

    /**
     * 生成快照内容
     */
    public Map<String/* schema */, String> snapshot();

}
