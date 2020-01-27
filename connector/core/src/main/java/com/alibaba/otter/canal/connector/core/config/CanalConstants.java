package com.alibaba.otter.canal.connector.core.config;

/**
 * 启动常用变量
 *
 * @author jianghang 2012-11-8 下午03:15:55
 * @version 1.0.0
 */
public class CanalConstants {

    public static final String ROOT                              = "canal";

    public static final String CANAL_FILTER_TRANSACTION_ENTRY    = ROOT + "." + "instance.filter.transaction.entry";

    public static final String CANAL_MQ_FLAT_MESSAGE             = ROOT + "." + "mq.flat.message";
    public static final String CANAL_MQ_DATABASE_HASH            = ROOT + "." + "mq.database.hash";
    public static final String CANAL_MQ_PARALLEL_THREAD_SIZE     = ROOT + "." + "mq.parallel.thread.size";
    public static final String CANAL_MQ_CANAL_BATCH_SIZE         = ROOT + "." + "mq.canal.batch.size";
    public static final String CANAL_MQ_CANAL_FETCH_TIMEOUT      = ROOT + "." + "mq.canal.fetch.timeout";
    public static final String CANAL_MQ_ACCESS_CHANNEL           = ROOT + "." + "mq.access.channel";
    public static final String CANAL_MQ_KAFKA_KERBEROS_ENABLE    = ROOT + "." + "mq.kafka.kerberos.enable";
    public static final String CANAL_MQ_KAFKA_KERBEROS_KRB5_FILE = ROOT + "." + "mq.kafka.kerberos.krb5.file";
    public static final String CANAL_MQ_KAFKA_KERBEROS_JAAS_FILE = ROOT + "." + "mq.kafka.kerberos.jaas.file";
}
