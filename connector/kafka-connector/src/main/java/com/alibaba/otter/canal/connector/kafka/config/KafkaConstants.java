package com.alibaba.otter.canal.connector.kafka.config;

/**
 * Kafka 配置常量类
 *
 * @author rewerma 2020-01-27
 * @version 1.0.0
 */
public class KafkaConstants {

    public static final String ROOT                              = "kafka";

    public static final String CANAL_MQ_KAFKA_KERBEROS_ENABLE    = ROOT + "." + "kerberos.enable";
    public static final String CANAL_MQ_KAFKA_KERBEROS_KRB5_FILE = ROOT + "." + "kerberos.krb5.file";
    public static final String CANAL_MQ_KAFKA_KERBEROS_JAAS_FILE = ROOT + "." + "kerberos.jaas.file";
}
