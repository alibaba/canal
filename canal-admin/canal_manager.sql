
SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for canal_adapter_config
-- ----------------------------
DROP TABLE IF EXISTS `canal_adapter_config`;
CREATE TABLE `canal_adapter_config` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `category` varchar(45) NOT NULL,
  `name` varchar(45) NOT NULL,
  `content` text NOT NULL,
  `modified_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Records of canal_adapter_config
-- ----------------------------
BEGIN;
INSERT INTO `canal_adapter_config` VALUES (1, 'es', 'mytest_user.yml', 'dataSourceKey: defaultDS\ndestination: example\nesMapping:\n  _index: mytest_user\n  _type: _doc\n  _id: _id\n#  pk: id\n  sql: \"select a.id as _id, a.name as _name, a.role_id as _role_id, b.role_name as _role_name,\n        a.c_time as _c_time, c.labels as _labels from user a\n        left join role b on b.id=a.role_id\n        left join (select user_id, group_concat(label order by id desc separator \';\') as labels from label\n        group by user_id) c on c.user_id=a.id\"\n#  objFields:\n#    _labels: array:;\n  etlCondition: \"where a.c_time>=\'{0}\'\"\n  commitBatch: 3000', '2019-01-25 18:48:22');
INSERT INTO `canal_adapter_config` VALUES (2, 'hbase', 'mytest_person2.yml', 'dataSourceKey: defaultDS\ndestination: example\nhbaseMapping:\n  mode: STRING  #NATIVE   #PHOENIX\n  database: mytest  # 数据库名\n  table: person2     # 数据库表名\n  hbaseTable: MYTEST.PERSON2   # HBase表名\n  family: CF  # 默认统一Family名称\n  uppercaseQualifier: true  # 字段名转大写, 默认为true\n  commitBatch: 3000 # 批量提交的大小\n  #rowKey: id,type  # 复合字段rowKey不能和columns中的rowKey重复\n  columns:\n    # 数据库字段:HBase对应字段\n    id: ROWKEY LEN:15\n    name: NAME\n    email: EMAIL\n    type:\n    c_time: C_TIME\n    birthday: BIRTHDAY\n#  excludeColumns:\n#    - lat   # 忽略字段\n\n# -- NATIVE类型\n# $DEFAULT\n# $STRING\n# $INTEGER\n# $LONG\n# $SHORT\n# $BOOLEAN\n# $FLOAT\n# $DOUBLE\n# $BIGDECIMAL\n# $DATE\n# $BYTE\n# $BYTES\n\n# -- PHOENIX类型\n# $DEFAULT                  对应PHOENIX里的VARCHAR\n# $UNSIGNED_INT             对应PHOENIX里的UNSIGNED_INT           4字节\n# $UNSIGNED_LONG            对应PHOENIX里的UNSIGNED_LONG          8字节\n# $UNSIGNED_TINYINT         对应PHOENIX里的UNSIGNED_TINYINT       1字节\n# $UNSIGNED_SMALLINT        对应PHOENIX里的UNSIGNED_SMALLINT      2字节\n# $UNSIGNED_FLOAT           对应PHOENIX里的UNSIGNED_FLOAT         4字节\n# $UNSIGNED_DOUBLE          对应PHOENIX里的UNSIGNED_DOUBLE        8字节\n# $INTEGER                  对应PHOENIX里的INTEGER                4字节\n# $BIGINT                   对应PHOENIX里的BIGINT                 8字节\n# $TINYINT                  对应PHOENIX里的TINYINT                1字节\n# $SMALLINT                 对应PHOENIX里的SMALLINT               2字节\n# $FLOAT                    对应PHOENIX里的FLOAT                  4字节\n# $DOUBLE                    对应PHOENIX里的DOUBLE                 8字节\n# $BOOLEAN                  对应PHOENIX里的BOOLEAN                1字节\n# $TIME                     对应PHOENIX里的TIME                   8字节\n# $DATE                     对应PHOENIX里的DATE                   8字节\n# $TIMESTAMP                对应PHOENIX里的TIMESTAMP              12字节\n# $UNSIGNED_TIME            对应PHOENIX里的UNSIGNED_TIME          8字节\n# $UNSIGNED_DATE            对应PHOENIX里的UNSIGNED_DATE          8字节\n# $UNSIGNED_TIMESTAMP       对应PHOENIX里的UNSIGNED_TIMESTAMP     12字节\n# $VARCHAR                  对应PHOENIX里的VARCHAR                动态长度\n# $VARBINARY                对应PHOENIX里的VARBINARY              动态长度\n# $DECIMAL                  对应PHOENIX里的DECIMAL                动态长度', '2019-01-14 10:58:13');
INSERT INTO `canal_adapter_config` VALUES (3, 'rdb', 'mytest_user.yml', 'dataSourceKey: defaultDS\ndestination: example\nouterAdapterKey: oracle1\nconcurrent: true\ndbMapping:\n  database: mytest\n  table: user\n  targetTable: mytest.tb_user\n  targetPk:\n    id: id\n#  mapAll: true\n  targetColumns:\n    id:\n    name:\n    role_id:\n    c_time:\n    test1:\n\n\n# Mirror schema synchronize config\n#dataSourceKey: defaultDS\n#destination: example\n#outerAdapterKey: mysql1\n#concurrent: true\n#dbMapping:\n#  mirrorDb: true\n#  database: mytest\n', '2019-01-01 17:08:50');
COMMIT;

-- ----------------------------
-- Table structure for canal_config
-- ----------------------------
DROP TABLE IF EXISTS `canal_config`;
CREATE TABLE `canal_config` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `name` varchar(45) NOT NULL,
  `content` text NOT NULL,
  `modified_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name_UNIQUE` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Records of canal_config
-- ----------------------------
BEGIN;
INSERT INTO `canal_config` VALUES (1, 'canal.properties', '#################################################\n######### 		common argument		#############\n#################################################\ncanal.manager.jdbc.url=jdbc:mysql://127.0.0.1:3306/canal_manager?useUnicode=true&characterEncoding=UTF-8\ncanal.manager.jdbc.username=root\ncanal.manager.jdbc.password=121212\n\ncanal.id = 1\ncanal.ip =\ncanal.port = 11111\ncanal.metrics.pull.port = 11112\ncanal.admin.jmx.port = 11113\ncanal.zkServers =\n# flush data to zk\ncanal.zookeeper.flush.period = 1000\ncanal.withoutNetty = false\n# tcp, kafka, RocketMQ\ncanal.serverMode = tcp\n# flush meta cursor/parse position to file\ncanal.file.data.dir = ${canal.conf.dir}\ncanal.file.flush.period = 1000\n## memory store RingBuffer size, should be Math.pow(2,n)\ncanal.instance.memory.buffer.size = 16384\n## memory store RingBuffer used memory unit size , default 1kb\ncanal.instance.memory.buffer.memunit = 1024 \n## meory store gets mode used MEMSIZE or ITEMSIZE\ncanal.instance.memory.batch.mode = MEMSIZE\ncanal.instance.memory.rawEntry = true\n\n## detecing config\ncanal.instance.detecting.enable = false\n#canal.instance.detecting.sql = insert into retl.xdual values(1,now()) on duplicate key update x=now()\ncanal.instance.detecting.sql = select 1\ncanal.instance.detecting.interval.time = 3\ncanal.instance.detecting.retry.threshold = 3\ncanal.instance.detecting.heartbeatHaEnable = false\n\n# support maximum transaction size, more than the size of the transaction will be cut into multiple transactions delivery\ncanal.instance.transaction.size =  1024\n# mysql fallback connected to new master should fallback times\ncanal.instance.fallbackIntervalInSeconds = 60\n\n# network config\ncanal.instance.network.receiveBufferSize = 16384\ncanal.instance.network.sendBufferSize = 16384\ncanal.instance.network.soTimeout = 30\n\n# binlog filter config\ncanal.instance.filter.druid.ddl = true\ncanal.instance.filter.query.dcl = false\ncanal.instance.filter.query.dml = false\ncanal.instance.filter.query.ddl = false\ncanal.instance.filter.table.error = false\ncanal.instance.filter.rows = false\ncanal.instance.filter.transaction.entry = false\n\n# binlog format/image check\ncanal.instance.binlog.format = ROW,STATEMENT,MIXED \ncanal.instance.binlog.image = FULL,MINIMAL,NOBLOB\n\n# binlog ddl isolation\ncanal.instance.get.ddl.isolation = false\n\n# parallel parser config\ncanal.instance.parser.parallel = true\n## concurrent thread number, default 60% available processors, suggest not to exceed Runtime.getRuntime().availableProcessors()\n#canal.instance.parser.parallelThreadSize = 16\n## disruptor ringbuffer size, must be power of 2\ncanal.instance.parser.parallelBufferSize = 256\n\n# table meta tsdb info\ncanal.instance.tsdb.enable = true\ncanal.instance.tsdb.dir = ${canal.file.data.dir:../conf}/${canal.instance.destination:}\ncanal.instance.tsdb.url = jdbc:h2:${canal.instance.tsdb.dir}/h2;CACHE_SIZE=1000;MODE=MYSQL;\ncanal.instance.tsdb.dbUsername = canal\ncanal.instance.tsdb.dbPassword = canal\n# dump snapshot interval, default 24 hour\ncanal.instance.tsdb.snapshot.interval = 24\n# purge snapshot expire , default 360 hour(15 days)\ncanal.instance.tsdb.snapshot.expire = 360\n\n# aliyun ak/sk , support rds/mq\ncanal.aliyun.accessKey =\ncanal.aliyun.secretKey =\n\n#################################################\n######### 		destinations		#############\n#################################################\ncanal.destinations = example\n# conf root dir\ncanal.conf.dir = ../conf\n# auto scan instance dir add/remove and start/stop instance\ncanal.auto.scan = true\ncanal.auto.scan.interval = 5\n\ncanal.instance.tsdb.spring.xml = classpath:spring/tsdb/h2-tsdb.xml\n#canal.instance.tsdb.spring.xml = classpath:spring/tsdb/mysql-tsdb.xml\n\ncanal.instance.global.mode = spring\ncanal.instance.global.lazy = false\n#canal.instance.global.manager.address = 127.0.0.1:1099\n#canal.instance.global.spring.xml = classpath:spring/memory-instance.xml\ncanal.instance.global.spring.xml = classpath:spring/file-instance.xml\n#canal.instance.global.spring.xml = classpath:spring/default-instance.xml\n\n##################################################\n######### 		     MQ 		     #############\n##################################################\ncanal.mq.servers = 127.0.0.1:6667\ncanal.mq.retries = 0\ncanal.mq.batchSize = 16384\ncanal.mq.maxRequestSize = 1048576\ncanal.mq.lingerMs = 100\ncanal.mq.bufferMemory = 33554432\ncanal.mq.canalBatchSize = 50\ncanal.mq.canalGetTimeout = 100\ncanal.mq.flatMessage = true\ncanal.mq.compressionType = none\ncanal.mq.acks = all\n#canal.mq.properties. =\ncanal.mq.producerGroup = test\n# Set this value to \"cloud\", if you want open message trace feature in aliyun.\ncanal.mq.accessChannel = local\n# aliyun mq namespace\n#canal.mq.namespace =\n\n##################################################\n#########     Kafka Kerberos Info    #############\n##################################################\ncanal.mq.kafka.kerberos.enable = false\ncanal.mq.kafka.kerberos.krb5FilePath = \"../conf/kerberos/krb5.conf\"\ncanal.mq.kafka.kerberos.jaasFilePath = \"../conf/kerberos/jaas.conf\"\n', '2019-07-14 09:44:20');
INSERT INTO `canal_config` VALUES (2, 'application.yml', 'server:\n  port: 8081\nlogging:\n  level:\n    org.springframework: WARN\n    com.alibaba.otter.canal.client.adapter.hbase: DEBUG\n    com.alibaba.otter.canal.client.adapter.es: DEBUG\n    com.alibaba.otter.canal.client.adapter.rdb: DEBUG\nspring:\n  jackson:\n    date-format: yyyy-MM-dd HH:mm:ss\n    time-zone: GMT+8\n    default-property-inclusion: non_null\n\ncanal.conf:\n  canalServerHost: 127.0.0.1:11111\n#  zookeeperHosts: 127.0.0.1:2181\n#  mqServers: 127.0.0.1:9092 #or rocketmq\n#  flatMessage: true\n  batchSize: 500\n  syncBatchSize: 1000\n  retries: 0\n  timeout:\n  accessKey:\n  secretKey:\n  mode: tcp # kafka rocketMQ\n#  srcDataSources:\n#    defaultDS:\n#      url: jdbc:mysql://127.0.0.1:3306/mytest?useUnicode=true\n#      username: root\n#      password: 121212\n  canalAdapters:\n  - instance: example # canal instance Name or mq topic name\n    groups:\n    - groupId: g1\n      outerAdapters:\n      - name: logger\n#      - name: rdb\n#        key: mysql1\n#        properties:\n#          jdbc.driverClassName: com.mysql.jdbc.Driver\n#          jdbc.url: jdbc:mysql://127.0.0.1:3306/mytest2?useUnicode=true\n#          jdbc.username: root\n#          jdbc.password: 121212\n#      - name: rdb\n#        key: oracle1\n#        properties:\n#          jdbc.driverClassName: oracle.jdbc.OracleDriver\n#          jdbc.url: jdbc:oracle:thin:@localhost:49161:XE\n#          jdbc.username: mytest\n#          jdbc.password: m121212\n#      - name: rdb\n#        key: postgres1\n#        properties:\n#          jdbc.driverClassName: org.postgresql.Driver\n#          jdbc.url: jdbc:postgresql://localhost:5432/postgres\n#          jdbc.username: postgres\n#          jdbc.password: 121212\n#          threads: 1\n#          commitSize: 3000\n#      - name: hbase\n#        properties:\n#          hbase.zookeeper.quorum: 127.0.0.1\n#          hbase.zookeeper.property.clientPort: 2181\n#          zookeeper.znode.parent: /hbase\n#      - name: es\n#        hosts: 127.0.0.1:9300\n#        properties:\n#          cluster.name: elasticsearch\n\n\n\n', '2019-01-25 18:47:38');
COMMIT;

-- ----------------------------
-- Table structure for canal_instance_config
-- ----------------------------
DROP TABLE IF EXISTS `canal_instance_config`;
CREATE TABLE `canal_instance_config` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `name` varchar(45) NOT NULL,
  `content` text NOT NULL,
  `modified_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name_UNIQUE` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Records of canal_instance_config
-- ----------------------------
BEGIN;
INSERT INTO `canal_instance_config` VALUES (1, 'example', '#################################################\n## mysql serverId , v1.0.26+ will autoGen\n# canal.instance.mysql.slaveId=0\n\n# enable gtid use true/false\ncanal.instance.gtidon=false\n\n# position info\ncanal.instance.master.address=127.0.0.1:3306\ncanal.instance.master.journal.name=\ncanal.instance.master.position=\ncanal.instance.master.timestamp=\ncanal.instance.master.gtid=\n\n# rds oss binlog\ncanal.instance.rds.accesskey=\ncanal.instance.rds.secretkey=\ncanal.instance.rds.instanceId=\n\n# table meta tsdb info\ncanal.instance.tsdb.enable=true\n#canal.instance.tsdb.url=jdbc:mysql://127.0.0.1:3306/canal_tsdb\n#canal.instance.tsdb.dbUsername=canal\n#canal.instance.tsdb.dbPassword=canal\n\n#canal.instance.standby.address =\n#canal.instance.standby.journal.name =\n#canal.instance.standby.position =\n#canal.instance.standby.timestamp =\n#canal.instance.standby.gtid=\n\n# username/password\ncanal.instance.dbUsername=canal\ncanal.instance.dbPassword=canal\ncanal.instance.connectionCharset = UTF-8\n# enable druid Decrypt database password\ncanal.instance.enableDruid=false\n#canal.instance.pwdPublicKey=MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBALK4BUxdDltRRE5/zXpVEVPUgunvscYFtEip3pmLlhrWpacX7y7GCMo2/JM6LeHmiiNdH1FWgGCpUfircSwlWKUCAwEAAQ==\n\n# table regex\ncanal.instance.filter.regex=.*\\\\..*\n# table black regex\ncanal.instance.filter.black.regex=\n\n# mq config\ncanal.mq.topic=example\n# dynamic topic route by schema or table regex\n#canal.mq.dynamicTopic=mytest1.user,mytest2\\\\..*,.*\\\\..*\ncanal.mq.partition=0\n# hash partition config\n#canal.mq.partitionsNum=3\n#canal.mq.partitionHash=test.table:id^name,.*\\\\..*\n#################################################\n', '2019-07-14 09:45:04');
COMMIT;

-- ----------------------------
-- Table structure for canal_node_server
-- ----------------------------
DROP TABLE IF EXISTS `canal_node_server`;
CREATE TABLE `canal_node_server` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `name` varchar(63) NOT NULL,
  `ip` varchar(63) NOT NULL,
  `port` int(11) DEFAULT NULL,
  `port2` int(11) DEFAULT NULL,
  `status` int(11) NOT NULL,
  `modified_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Table structure for canal_user
-- ----------------------------
DROP TABLE IF EXISTS `canal_user`;
CREATE TABLE `canal_user` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `username` varchar(31) NOT NULL,
  `password` varchar(128) NOT NULL,
  `name` varchar(31) NOT NULL,
  `roles` varchar(31) NOT NULL,
  `introduction` varchar(255) DEFAULT NULL,
  `avatar` varchar(255) DEFAULT NULL,
  `creation_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Records of canal_user
-- ----------------------------
BEGIN;
INSERT INTO `canal_user` VALUES (1, 'admin', '121212', 'Canal Manager', 'admin', NULL, NULL, '2019-07-14 00:05:28');
COMMIT;

SET FOREIGN_KEY_CHECKS = 1;
