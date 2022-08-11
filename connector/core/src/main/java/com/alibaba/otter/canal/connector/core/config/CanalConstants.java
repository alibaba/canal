package com.alibaba.otter.canal.connector.core.config;

/**
 * MQ配置常量
 *
 * @author rewerma 2020-01-27
 * @version 1.0.0
 */
public class CanalConstants {

    public static final String ROOT                           = "canal";

    public static final String CANAL_FILTER_TRANSACTION_ENTRY = ROOT + "." + "instance.filter.transaction.entry";

    public static final String CANAL_MQ_ACCESS_CHANNEL        = ROOT + "." + "mq.accessChannel";
    public static final String CANAL_MQ_CANAL_BATCH_SIZE      = ROOT + "." + "mq.canalBatchSize";
    public static final String CANAL_MQ_CANAL_GET_TIMEOUT     = ROOT + "." + "mq.canalGetTimeout";
    public static final String CANAL_MQ_FLAT_MESSAGE          = ROOT + "." + "mq.flatMessage";

    public static final String CANAL_MQ_DATABASE_HASH         = ROOT + "." + "mq.database.hash";
    public static final String CANAL_MQ_BUILD_THREAD_SIZE     = ROOT + "." + "mq.build.thread.size";
    public static final String CANAL_MQ_SEND_THREAD_SIZE      = ROOT + "." + "mq.send.thread.size";

    public static final String CANAL_ALIYUN_ACCESS_KEY        = ROOT + "." + "aliyun.accessKey";
    public static final String CANAL_ALIYUN_SECRET_KEY        = ROOT + "." + "aliyun.secretKey";
    public static final String CANAL_ALIYUN_UID               = ROOT + "." + "aliyun.uid";

}
