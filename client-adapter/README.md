## 基本说明
canal 1.1.1版本之后, 增加客户端数据落地的适配及启动功能, 目前支持功能:
* 客户端启动器
* 同步管理REST接口
* 日志适配器, 作为DEMO
* HBase的数据同步(表对表同步), ETL功能
* (后续支持) ElasticSearch多表数据同步,ETL功能

## 环境版本
* 操作系统：无要求
* java版本: jdk1.8 以上 
* canal 版本: 请下载最新的安装包，本文以当前v1.1.1 的canal.deployer-1.1.1.tar.gz为例
* MySQL版本 ：5.7.18
* HBase版本: Apache HBase 1.1.2, 若和服务端版本不一致可自行替换客户端HBase依赖

## 一、适配器启动器
client-adapter分为适配器和启动器两部分, 适配器为多个fat jar, 每个适配器会将自己所需的依赖打成一个包, 以SPI的方式让启动器动态加载


启动器为 SpringBoot 项目, 支持canal-client启动的同时提供相关REST管理接口, 运行目录结构为:
```
canal-adapter-launcher.jar
- lib
    client-adapter.logger-1.1.1-jar-with-dependencies.jar
    client-adapter.hbase-1.1.1-jar-with-dependencies.jar
- config
    application.yml
    - hbase
        mytest_person2.yml
```
以上目录结构最终会打包成 canal-adapter-launcher.tar.gz 压缩包

## 二、启动器
### 2.1 启动器配置 application.yml
#### canal相关配置部分说明
```
canal.conf:
  canalServerHost: 127.0.0.1:11111  # 对应单机模式下的canal server的ip:port
  zookeeperHosts: slave1:2181       # 对应集群模式下的zk地址, 如果配置了canalServerHost, 则以canalServerHost为准
  bootstrapServers: slave1:6667     # kafka或rocketMQ地址, 与canalServerHost不能并存
  flatMessage: true                 # 扁平message开关, 是否以json字符串形式投递数据, 仅在kafka/rocketMQ模式下有效
  canalInstances:                   # canal实例组, 如果是tcp模式可配置此项
  - instance: example               # 对应canal destination
    groups:                  # 对应适配器分组, 分组间的适配器并行运行
    - outAdapters:                  # 适配器列表, 分组内的适配串行运行
      - name: logger                # 适配器SPI名
      - name: hbase
        properties:                 # HBase相关连接参数
          hbase.zookeeper.quorum: slave1
          hbase.zookeeper.property.clientPort: 2181
          zookeeper.znode.parent: /hbase
  mqTopics:                         # MQ topic租, 如果是kafka或者rockeMQ模式可配置此项, 与canalInstances不能并存
  - mqMode: kafka                   # MQ的模式: kafak/rocketMQ
    topic: example                  # MQ topic
    groups:                         # group组
    - groupId: g2                   # group id
      outAdapters:                  # 适配器列表, 以下配置和canalInstances中一样
      - name: logger                
```
#### 适配器相关配置部分说明
```
adapter.conf:
  datasourceConfigs:                # 数据源配置列表, 数据源将在适配器中用于ETL、数据同步回查等使用
    defaultDS:                      # 数据源 dataSourceKey, 适配器中通过该值获取指定数据源
      url: jdbc:mysql://127.0.0.1:3306/mytest?useUnicode=true
      username: root
      password: 121212
  adapterConfigs:                   # 适配器内部配置列表
  - hbase/mytest_person2.yml        # 类型/配置文件名, 这里示例就是对应HBase适配器hbase目录下的mytest_person2.yml文件
```
## 2.2 同步管理REST接口
#### 2.2.1 查询所有订阅同步的canal destination或MQ topic
```
curl http://127.0.0.1:8081/destinations
```
#### 2.2.2 数据同步开关
```
curl http://127.0.0.1:8081/syncSwitch/example/off -X PUT
```
针对 example 这个canal destination/MQ topic 进行开关操作. off代表关闭, 该destination/topic下的同步将阻塞或者断开连接不再接收数据, on代表开启

注: 如果在配置文件中配置了 zookeeperHosts 项, 则会使用分布式锁来控制HA中的数据同步开关, 如果是单机模式则使用本地锁来控制开关
#### 2.2.3 数据同步开关状态
```
curl http://127.0.0.1:8081/syncSwitch/example
```
查看指定 canal destination/MQ topic 的数据同步开关状态
#### 2.2.4 手动ETL
```
curl http://127.0.0.1:8081/etl/hbase/mytest_person2.yml -X POST -d "params=2018-10-21 00:00:00"
```
导入数据到指定类型的库
#### 2.2.5 查看相关库总数据
```
curl http://127.0.0.1:8081/count/hbase/mytest_person2.yml
```
### 2.3 启动canal-adapter示例
#### 2.3.1 启动canal server (单机模式), 参考: [Canal QuickStart](https://github.com/alibaba/canal/wiki/QuickStart)
#### 2.3.2 修改config/application.yml为:
```
server:
  port: 8081
logging:
  level:
    com.alibaba.otter.canal.client.adapter.hbase: DEBUG
spring:
  jackson:
    date-format: yyyy-MM-dd HH:mm:ss
    time-zone: GMT+8
    default-property-inclusion: non_null

canal.conf:
  canalServerHost: 127.0.0.1:11111
  flatMessage: true
  canalInstances:
  - instance: example
    adapterGroups:
    - outAdapters:
      - name: logger
```
启动 canal-adapter-launcher.jar
```
java -jar canal-adapter-launcher.jar
```

## 三、HBase适配器
### 3.1 修改启动器配置: application.yml
```
server:
  port: 8081
logging:
  level:
    com.alibaba.otter.canal.client.adapter.hbase: DEBUG
spring:
  jackson:
    date-format: yyyy-MM-dd HH:mm:ss
    time-zone: GMT+8
    default-property-inclusion: non_null

canal.conf:
  canalServerHost: 127.0.0.1:11111
  flatMessage: true
  canalInstances:
  - instance: example
    adapterGroups:
    - outAdapters:
      - name: hbase
        properties:
          hbase.zookeeper.quorum: slave1
          hbase.zookeeper.property.clientPort: 2181
          zookeeper.znode.parent: /hbase

adapter.conf:
  datasourceConfigs:
    defaultDS:
      url: jdbc:mysql://127.0.0.1:3306/mytest?useUnicode=true
      username: root
      password: 121212
  adapterConfigs:
  - hbase/mytest_person.yml 
```
其中指定了一个HBase表映射文件: mytest_person.yml
### 3.2 适配器表映射文件
修改 config/hbase/mytest_person.yml文件:
```
dataSourceKey: defaultDS            # 对应application.yml中的datasourceConfigs下的配置
hbaseMapping:                       # mysql--HBase的单表映射配置
  mode: STRING                      # HBase中的存储类型, 默认统一存为String, 可选: #PHOENIX  #NATIVE   #STRING 
                                    # NATIVE: 以java类型为主, PHOENIX: 将类型转换为Phoenix对应的类型
  destination: example              # 对应 canal destination/MQ topic 名称
  database: mytest                  # 数据库名/schema名
  table: person                     # 表名
  hbaseTable: MYTEST.PERSON         # HBase表名
  family: CF                        # 默认统一Column Family名称
  uppercaseQualifier: true          # 字段名转大写, 默认为true
  commitBatch: 3000                 # 批量提交的大小, ETL中用到
  #rowKey: id,type                  # 复合字段rowKey不能和columns中的rowKey并存
                                    # 复合rowKey会以 '|' 分隔
  columns:                          # 字段映射, 如果不配置将自动映射所有字段, 
                                    # 并取第一个字段为rowKey, HBase字段名以mysql字段名为主
    id: ROWKE                       
    name: CF:NAME
    email: EMAIL                    # 如果column family为默认CF, 则可以省略
    type:                           # 如果HBase字段和mysql字段名一致, 则可以省略
    c_time: 
    birthday: 
```
如果涉及到类型转换,可以如下形式:
```
...
  columns:                         
    id: ROWKE$STRING                      
    ...                   
    type: TYPE$BYTE                          
    ...
```
类型转换涉及到Java类型和Phoenix类型两种, 分别定义如下:
```
#Java 类型转换, 对应配置 mode: NATIVE
$DEFAULT
$STRING
$INTEGER
$LONG
$SHORT
$BOOLEAN
$FLOAT
$DOUBLE
$BIGDECIMAL
$DATE
$BYTE
$BYTES
```
```
#Phoenix 类型转换, 对应配置 mode: PHOENIX
$DEFAULT                  对应PHOENIX里的VARCHAR
$UNSIGNED_INT             对应PHOENIX里的UNSIGNED_INT           4字节
$UNSIGNED_LONG            对应PHOENIX里的UNSIGNED_LONG          8字节
$UNSIGNED_TINYINT         对应PHOENIX里的UNSIGNED_TINYINT       1字节
$UNSIGNED_SMALLINT        对应PHOENIX里的UNSIGNED_SMALLINT      2字节
$UNSIGNED_FLOAT           对应PHOENIX里的UNSIGNED_FLOAT         4字节
$UNSIGNED_DOUBLE          对应PHOENIX里的UNSIGNED_DOUBLE        8字节
$INTEGER                  对应PHOENIX里的INTEGER                4字节
$BIGINT                   对应PHOENIX里的BIGINT                 8字节
$TINYINT                  对应PHOENIX里的TINYINT                1字节
$SMALLINT                 对应PHOENIX里的SMALLINT               2字节
$FLOAT                    对应PHOENIX里的FLOAT                  4字节
$DOUBLE                   对应PHOENIX里的DOUBLE                 8字节
$BOOLEAN                  对应PHOENIX里的BOOLEAN                1字节
$TIME                     对应PHOENIX里的TIME                   8字节
$DATE                     对应PHOENIX里的DATE                   8字节
$TIMESTAMP                对应PHOENIX里的TIMESTAMP              12字节
$UNSIGNED_TIME            对应PHOENIX里的UNSIGNED_TIME          8字节
$UNSIGNED_DATE            对应PHOENIX里的UNSIGNED_DATE          8字节
$UNSIGNED_TIMESTAMP       对应PHOENIX里的UNSIGNED_TIMESTAMP     12字节
$VARCHAR                  对应PHOENIX里的VARCHAR                动态长度
$VARBINARY                对应PHOENIX里的VARBINARY              动态长度
$DECIMAL                  对应PHOENIX里的DECIMAL                动态长度
```
如果不配置将以java对象原生类型默认映射转换
### 3.3 启动HBase数据同步
#### 创建HBase表
在HBase shell中运行:
```
create 'MYTEST.PERSON', {NAME=>'CF'}
```
#### 启动canal-adapter启动器
```
java -jar canal-adapter-launcher.jar
```
#### 验证
修改mysql mytest.person表的数据, 将会自动同步到HBase的MYTEST.PERSON表下面, 并会打出DML的log
