package com.alibaba.otter.canal.client.example;

import com.alibaba.otter.canal.client.adapter.CanalOuterAdapter;
import com.alibaba.otter.canal.client.adapter.CanalOuterAdapterConfiguration;
import com.alibaba.otter.canal.client.support.Dml;
import com.alibaba.otter.canal.client.support.SPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 外部适配器示例
 *
 * @author machengyuan 2018-8-19 下午11:45:38
 * @version 1.0.0
 */
@SPI("logger") // logger参数对应CanalOuterAdapterConfiguration配置中的name
public class LoggerAdapterExample implements CanalOuterAdapter {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public Boolean writeOut(Dml dml) {
        // 直接输出一个日志信息
        logger.info(dml.toString());
        return true;
    }

    @Override
    public void init(CanalOuterAdapterConfiguration configuration) {

    }

    @Override
    public void destroy() {

    }
}
