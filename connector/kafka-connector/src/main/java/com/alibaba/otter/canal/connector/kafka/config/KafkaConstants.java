package com.alibaba.otter.canal.connector.kafka.config;

/**
 * 启动常用变量
 *
 * @author jianghang 2012-11-8 下午03:15:55
 * @version 1.0.0
 */
public class KafkaConstants {

    public static final String ROOT                              = "canal";

    public static final String CANAL_MQ_KAFKA_KERBEROS_ENABLE    = ROOT + "." + "mq.kafka.kerberos.enable";
    public static final String CANAL_MQ_KAFKA_KERBEROS_KRB5_FILE = ROOT + "." + "mq.kafka.kerberos.krb5.file";
    public static final String CANAL_MQ_KAFKA_KERBEROS_JAAS_FILE = ROOT + "." + "mq.kafka.kerberos.jaas.file";
}
