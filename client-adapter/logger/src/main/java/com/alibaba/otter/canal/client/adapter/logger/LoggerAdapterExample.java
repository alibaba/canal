package com.alibaba.otter.canal.client.adapter.logger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.SPI;

/**
 * 外部适配器示例
 *
 * @author machengyuan 2018-8-19 下午11:45:38
 * @version 1.0.0
 */
@SPI("logger")
// logger参数对应CanalOuterAdapterConfiguration配置中的name
public class LoggerAdapterExample implements OuterAdapter {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void init(OuterAdapterConfig configuration) {

    }

    @Override
    public void sync(Dml dml) {
        logger.info("DML: {}", dml.toString());
    }

    @Override
    public void destroy() {

    }
}
