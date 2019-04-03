package com.alibaba.otter.canal.client.adapter.logger;

import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;
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
    public void init(OuterAdapterConfig configuration, Properties envProperties) {
    }

    public void sync(List<Dml> dmls) {
        for (Dml dml : dmls) {
            sync(dml);
        }
    }

    public void sync(Dml dml) {
        logger.info("DML: {}", JSON.toJSONString(dml, SerializerFeature.WriteMapNullValue));
    }

    @Override
    public void destroy() {
    }
}
