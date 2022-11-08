package com.alibaba.otter.canal.deployer;

import java.text.MessageFormat;

/**
 * 启动常用变量
 *
 * @author jianghang 2012-11-8 下午03:15:55
 * @version 1.0.0
 */
public class CanalConstants {

    public static final String MDC_DESTINATION                      = "destination";
    public static final String ROOT                                 = "canal";
    public static final String CANAL_ID                             = ROOT + "." + "id";
    public static final String CANAL_IP                             = ROOT + "." + "ip";
    public static final String CANAL_REGISTER_IP                    = ROOT + "." + "register.ip";
    public static final String CANAL_PORT                           = ROOT + "." + "port";
    public static final String CANAL_USER                           = ROOT + "." + "user";
    public static final String CANAL_PASSWD                         = ROOT + "." + "passwd";
    public static final String CANAL_METRICS_PULL_PORT              = ROOT + "." + "metrics.pull.port";
    public static final String CANAL_ADMIN_MANAGER                  = ROOT + "." + "admin.manager";
    public static final String CANAL_ADMIN_PORT                     = ROOT + "." + "admin.port";
    public static final String CANAL_ADMIN_USER                     = ROOT + "." + "admin.user";
    public static final String CANAL_ADMIN_PASSWD                   = ROOT + "." + "admin.passwd";
    public static final String CANAL_ADMIN_AUTO_REGISTER            = ROOT + "." + "admin.register.auto";
    public static final String CANAL_ADMIN_AUTO_CLUSTER             = ROOT + "." + "admin.register.cluster";
    public static final String CANAL_ADMIN_REGISTER_NAME            = ROOT + "." + "admin.register.name";
    public static final String CANAL_ZKSERVERS                      = ROOT + "." + "zkServers";
    public static final String CANAL_WITHOUT_NETTY                  = ROOT + "." + "withoutNetty";

    public static final String CANAL_DESTINATIONS                   = ROOT + "." + "destinations";
    public static final String CANAL_AUTO_SCAN                      = ROOT + "." + "auto.scan";
    public static final String CANAL_AUTO_SCAN_INTERVAL             = ROOT + "." + "auto.scan.interval";
    public static final String CANAL_CONF_DIR                       = ROOT + "." + "conf.dir";
    public static final String CANAL_SERVER_MODE                    = ROOT + "." + "serverMode";

    public static final String CANAL_DESTINATION_SPLIT              = ",";
    public static final String GLOBAL_NAME                          = "global";

    public static final String INSTANCE_MODE_TEMPLATE               = ROOT + "." + "instance.{0}.mode";
    public static final String INSTANCE_LAZY_TEMPLATE               = ROOT + "." + "instance.{0}.lazy";
    public static final String INSTANCE_MANAGER_ADDRESS_TEMPLATE    = ROOT + "." + "instance.{0}.manager.address";
    public static final String INSTANCE_SPRING_XML_TEMPLATE         = ROOT + "." + "instance.{0}.spring.xml";

    public static final String CANAL_DESTINATION_PROPERTY           = ROOT + ".instance.destination";

    public static final String CANAL_SOCKETCHANNEL                  = ROOT + "." + "socketChannel";

    public static final String CANAL_ALIYUN_ACCESSKEY               = ROOT + "." + "aliyun.accessKey";
    public static final String CANAL_ALIYUN_SECRETKEY               = ROOT + "." + "aliyun.secretKey";

//    public static final String CANAL_MQ_SERVERS                     = ROOT + "." + "mq.servers";
//    public static final String CANAL_MQ_RETRIES                     = ROOT + "." + "mq.retries";
//    public static final String CANAL_MQ_BATCHSIZE                   = ROOT + "." + "mq.batchSize";
//    public static final String CANAL_MQ_LINGERMS                    = ROOT + "." + "mq.lingerMs";
//    public static final String CANAL_MQ_MAXREQUESTSIZE              = ROOT + "." + "mq.maxRequestSize";
//    public static final String CANAL_MQ_BUFFERMEMORY                = ROOT + "." + "mq.bufferMemory";
//    public static final String CANAL_MQ_CANALBATCHSIZE              = ROOT + "." + "mq.canalBatchSize";
//    public static final String CANAL_MQ_CANALGETTIMEOUT             = ROOT + "." + "mq.canalGetTimeout";
//    public static final String CANAL_MQ_FLATMESSAGE                 = ROOT + "." + "mq.flatMessage";
//    public static final String CANAL_MQ_PARALLELTHREADSIZE          = ROOT + "." + "mq.parallelThreadSize";
//    public static final String CANAL_MQ_COMPRESSION_TYPE            = ROOT + "." + "mq.compressionType";
//    public static final String CANAL_MQ_ACKS                        = ROOT + "." + "mq.acks";
//    public static final String CANAL_MQ_TRANSACTION                 = ROOT + "." + "mq.transaction";
//    public static final String CANAL_MQ_PRODUCERGROUP               = ROOT + "." + "mq.producerGroup";
//    public static final String CANAL_MQ_PROPERTIES                  = ROOT + "." + "mq.properties";
//    public static final String CANAL_MQ_ENABLE_MESSAGE_TRACE        = ROOT + "." + "mq.enableMessageTrace";
//    public static final String CANAL_MQ_ACCESS_CHANNEL              = ROOT + "." + "mq.accessChannel";
//    public static final String CANAL_MQ_CUSTOMIZED_TRACE_TOPIC      = ROOT + "." + "mq.customizedTraceTopic";
//    public static final String CANAL_MQ_NAMESPACE                   = ROOT + "." + "mq.namespace";
//    public static final String CANAL_MQ_KAFKA_KERBEROS_ENABLE       = ROOT + "." + "mq.kafka.kerberos.enable";
//    public static final String CANAL_MQ_KAFKA_KERBEROS_KRB5FILEPATH = ROOT + "." + "mq.kafka.kerberos.krb5FilePath";
//    public static final String CANAL_MQ_KAFKA_KERBEROS_JAASFILEPATH = ROOT + "." + "mq.kafka.kerberos.jaasFilePath";
//    public static final String CANAL_MQ_USERNAME                    = ROOT + "." + "mq.username";
//    public static final String CANAL_MQ_PASSWORD                    = ROOT + "." + "mq.password";
//    public static final String CANAL_MQ_VHOST                       = ROOT + "." + "mq.vhost";
//    public static final String CANAL_MQ_ALIYUN_UID                  = ROOT + "." + "mq.aliyunuid";
//    public static final String CANAL_MQ_EXCHANGE                    = ROOT + "." + "mq.exchange";
//    public static final String CANAL_MQ_DATABASE_HASH               = ROOT + "." + "mq.database.hash";

    public static String getInstanceModeKey(String destination) {
        return MessageFormat.format(INSTANCE_MODE_TEMPLATE, destination);
    }

    public static String getInstanceManagerAddressKey(String destination) {
        return MessageFormat.format(INSTANCE_MANAGER_ADDRESS_TEMPLATE, destination);
    }

    public static String getInstancSpringXmlKey(String destination) {
        return MessageFormat.format(INSTANCE_SPRING_XML_TEMPLATE, destination);
    }

    public static String getInstancLazyKey(String destination) {
        return MessageFormat.format(INSTANCE_LAZY_TEMPLATE, destination);
    }
}
