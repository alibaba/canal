package com.alibaba.otter.canal.client.adapter.logger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.adapter.CanalOuterAdapter;
import com.alibaba.otter.canal.client.adapter.support.CanalOuterAdapterConfiguration;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.MessageUtil;
import com.alibaba.otter.canal.client.adapter.support.SPI;
import com.alibaba.otter.canal.protocol.Message;

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
    public void writeOut(Message message) {
        // 直接输出日志信息
        MessageUtil.parse4Dml(message, new MessageUtil.Consumer<Dml>() {

            @Override
            public void accept(Dml dml) {
                logger.info(dml.toString());
            }
        });
    }

    @Override
    public void init(CanalOuterAdapterConfiguration configuration) {

    }

    @Override
    public void destroy() {

    }
}
