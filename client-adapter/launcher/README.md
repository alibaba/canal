# 记录测试数据，插入订单表
```sql
INSERT INTO `order`.`order1` (`order_id`, `event_id`, `placed_by_id`, `placed_by`, `placed_method`, `order_type`, `is_renewed`, `has_after_sale`, `is_exchanged`, `has_gifts`, `is_group`, `delay_shipping`, `group_code`, `state`, `order_remark`, `customer_id`, `customer_phone_number`, `encrypted_customer_phone_number`, `hashed_customer_phone_number`, `customer_union_id`, `customer_open_id`, `third_party_order_id`, `platform`, `currency`, `overseas_order`, `paid_deposit`, `total_discount`, `original_total`, `subtotal`, `total`, `actual_payment`, `payment_option`, `first_paid_at`, `paid_at`, `expired_at`, `closed_at`, `cancelled_at`, `finished_at`, `cancellation_reason`, `closed_reason_id`, `closed_reason`, `review_state`, `payment_state`, `check_state`, `shipping_state`, `contract_id`, `business_type_id`, `business_unit_id`, `business_code`, `use_points`, `shipped_at`, `placed_at`, `updated_at`)
VALUES ('CM200430157802412232387889150', 2, 241873, '高兰test', 2, 2, 1, 0, 0, 0, 0, 0, '', 127, '自动化测试创建订单', 1000845917, '', '00$$sgCKSRQ0jc8Jfc6S3rt+OQ==', '4683d12884f5175b494409b538d1519e38341ac3e24e6c35ed406d078e8f85bb', '', '240701', '', 5, 'CNY', 0, 0.00, 0.00, 0.10, 0.10, 0.10, 0.10, 1, 1588215781, 1588215781, 0, 0, 0, 0, '', 0, '', 105, 206, 303, 403, 0, 1, 13, 'ROCKET_headteacher', 100000000, 1588215782, 1588215780, 1617250363);

INSERT INTO `order`.`order_performance` (`order_id`, `order_type`, `owner_id`, `owner`, `handled_by_id`, `handled_by`, `created_at`, `updated_at`)
VALUES ('CM200430157802412232387889150', 2, 241873, '高珂', 0, '', 1632981600, 1632981600);
```

# 配置文件

## application.yml
```yaml
server:
  port: 18081
spring:
  jackson:
    date-format: yyyy-MM-dd HH:mm:ss
    time-zone: GMT+8
    default-property-inclusion: non_null
logging:
  level:
    com.alibaba.otter.canal.client.adapter.es: DEBUG

canal.conf:
  mode: kafka #tcp kafka rocketMQ rabbitMQ
  flatMessage: false
  zookeeperHosts:
  syncBatchSize: 1000
  retries: 0
  timeout:
  accessKey:
  secretKey:
  consumerProperties:
    # canal tcp consumer
    canal.tcp.server.host: 127.0.0.1:11111
    canal.tcp.zookeeper.hosts:
    canal.tcp.batch.size: 500
    canal.tcp.username:
    canal.tcp.password:
    # kafka consumer
    kafka.bootstrap.servers: 127.0.0.1:9092
    kafka.enable.auto.commit: false
    kafka.auto.commit.interval.ms: 1000
    kafka.auto.offset.reset: latest
    kafka.request.timeout.ms: 40000
    kafka.session.timeout.ms: 30000
    kafka.isolation.level: read_committed
    kafka.max.poll.records: 1000
    # rocketMQ consumer
    rocketmq.namespace:
    rocketmq.namesrv.addr: 127.0.0.1:9876
    rocketmq.batch.size: 1000
    rocketmq.enable.message.trace: false
    rocketmq.customized.trace.topic:
    rocketmq.access.channel:
    rocketmq.subscribe.filter:
    # rabbitMQ consumer
    rabbitmq.host:
    rabbitmq.virtual.host:
    rabbitmq.username:
    rabbitmq.password:
    rabbitmq.resource.ownerId:

  srcDataSources:
    defaultDS:
      url: jdbc:mysql://127.0.0.1:3306/order?useUnicode=true
      username: root
      password: rootroot
  canalAdapters:
    - instance: example # canal instance Name or mq topic name
      groups:
        - groupId: g4
          outerAdapters:
            - name: logger
            - name: es7
              key: order
              hosts: http://127.0.0.1:9200 # 127.0.0.1:9200 for rest mode，切记，一定要带上协议(http://)
              properties:
                mode: rest # rest, transport 注意：transport有问题，需要解决lucene50问题
#               security.auth: test:root #  only used for rest mode，用于连接elasticsearch的密码
                cluster.name: codemao #集群名
```

## order.yml
```yaml
dataSourceKey: defaultDS  #对应application.yml的canal数据源，可实现通过数据源来动态加载配置
outerAdapterKey: order #对应application.yml的adapter的key
destination: example  #MQ的topic
groupId: g4
esMapping:
  _index: order
  _id: id  #这个字段需要在下面的sql中体现出来，会作为一个映射，增量更新时会在下面的sql加上一个where order1.id=xxx的语句
  sql: "select ANY_VALUE(order1.id) as id, ANY_VALUE(order1.order_id) as order_id, ANY_VALUE(order1.placed_at) as placed_at, ANY_VALUE(order1.updated_at) as updated_at, ANY_VALUE(order1.placed_by_id) as placed_by_id, ANY_VALUE(performance.owner_id) as owner_id, ANY_VALUE(employee.department_id) as department_id ,MIN(organization.admin_tree_path) AS admin_tree_path from order1 LEFT JOIN `order`.order_performance performance on performance.order_id = order1.order_id LEFT JOIN internal_account.user_employee  employee ON employee.user_id = performance.owner_id LEFT JOIN ( SELECT id, admin_tree_path FROM internal_account.organization WHERE organization.admin_tree_path IS NOT NULL ) organization ON organization.id = employee.department_id"
  etlCondition: "where updated_at>={}"  #用于etl功能，可以直接调用接口操作，{}为接口的传参，写死无效，必须通过接口传
  commitBatch: 3000
```


# 坑点记录
实现跨库多表查询总结
1、主要是配置文件隐藏了太多细节（order.yml） 
* mode为：transport出错这一步花了比较多的时间
* 主要是SQL语句出错后的调试花了比较多的时间
* 加载yaml文件的方式是通过spring加载yaml property，通过反射直接实现的加载
* sql的解析是通过druid解析的，所以问题藏得比较深
* sql查询最后他会加一个主键查询条件

2、application.yml
* transport改为rest模式
* hosts需要加上http://  （协议）
* 修改子module的文件后需要重新clean和install adapter后launcher才能生效
* 修改ESSyncUtil文件内的拼接主键的判断条件，有一个owner判断的代码，修改了条件才跑成功

# 参考文章
* https://blog.csdn.net/puhaiyang/article/details/100171395
* https://github.com/alibaba/canal/issues/3727
* https://github.com/alibaba/canal/pull/3723
* https://github.com/alibaba/canal/pull/3723
* https://github.com/alibaba/canal/issues/3328
* https://github.com/alibaba/canal/issues/2023 
    如果修改主表字段，从表刚好有同名字段，ES中会一并修改从表对应的字段#2023
* In aggregated query without GROUP BY, expression #1 of SELECT list contains nonaggregated column 'order.order1.id'; this is incompatible with sql_mode=only_full_group_by
* https://docs.oracle.com/cd/E17952_01/mysql-5.6-en/group-by-handling.html
* https://zhuanlan.zhihu.com/p/66281594   有赞亿级订单同步的探索与实践
* https://juejin.cn/post/6945784603508637709  Canal 解决 MySQL 和 Redis 数据同步问题
* https://blog.csdn.net/u014730658/article/details/108399658  基于canal的client-adapter数据同步必读指南
