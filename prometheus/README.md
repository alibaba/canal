###Canal Prometheus 简介

Canal server 性能指标监控基于prometheus的实现。

关于prometheus，参见[官网](https://prometheus.io/)

#### 当前监控的canal相关原始指标列表：

|  指标                         | 说明            |  单位  | 精度 |
| :----                        | :----           | ----: |----: |
|canal_instance_traffic_delay |instance延迟    | 毫秒  |  毫秒 |
|canal_instance_transactions |instance接收transactions计数|-|-|
|canal_instance_row_events |instance接收rowData类型events计数|-|-|
|canal_instance_rows_counter |instance接收events包含变更行数计数|-|-|
|canal_instance |instance基本信息|-|-|
|canal_instance_subscriptions |instance订阅数量|-|-|
|canal_instance_publish_blocking_time |instance dump线程publish阻塞时间(仅parallel解析模式)|毫秒|纳秒|
|canal_instance_received_binlog_bytes |instance接收binlog字节数|byte|-|
|canal_instance_parser_mode |instance解析模式(是否开启parallel解析)|-|-|
|canal_instance_client_packets |instance client请求packets计数|-|-|
|canal_instance_client_bytes|向instance client发送数据包字节计数|byte|-|
|canal_instance_client_empty_batches|向instance client发送的空batch计数|-|-|
|canal_instance_client_request_error|instance client请求失败计数|-|-|
|canal_instance_client_request_latency |instance client请求延迟概况|-|-|
|canal_instance_sink_blocking_time |instance sink线程put数据至store的阻塞时间|毫秒|纳秒|
|canal_instance_store_produce_seq |instance store接收到的events sequence number|-|-|
|canal_instance_store_consume_seq |instance store成功消费的events sequence number|-|-|
|canal_instance_store |instance store基本信息|-|-|
|canal_instance_store_produce_mem |instance store接收到的所有events占用内存总量|byte|-|
|canal_instance_store_consume_mem |instance store成功消费的所有events占用内存总量|byte|-|

#### JVM 相关信息
> The Java client includes collectors for garbage collection, memory pools, JMX, classloading, and thread counts. These can be added individually or just use the DefaultExports to conveniently register them.
> >DefaultExports.initialize();

详见：[prometheus/client_java](https://github.com/prometheus/client_java)


#### Quick start

1. 安装并部署对应平台的prometheus，参见[官方guide](https://prometheus.io/docs/introduction/first_steps/)

2. 配置prometheus.yml，添加canal的job，示例:
```
  - job_name: 'canal'
    static_configs:
    - targets: ['localhost:11112'] //端口配置即为canal.properties中的canal.metrics.pull.port
```

3. 启动prometheus与canal server

#### Prometheus expression 与相应 use case
* Sink线程空闲比
```
clamp_max(rate(canal_instance_sink_blocking_time{destination="example"}[2m]), 1000) / 10
```
**sink线程idle时间片比例(向store中put events时)。若idle占比很高，则store总体上处于满的状态，client的consume速度低于server的produce速度**

_简单说明一下range-vector,通俗来说表达式会用时间点前range-vector period内的所有samples参与运算。_
_range-vector值如果太小，图会碎片化；反之，实时性会比较差。请结合scrape_interval设定合理的值。_

* Dump线程空闲比
```
clamp_max(rate(canal_instance_publish_blocking_time{destination="example"}[2m]), 1000) / 10
```
**dump线程idle时间片比例(仅parallel mode, dump线程向disruptor发布event时)。若idle占比较高：**

**1. Sinking idle ratio也很高，则总体还是因为client的consume速度相对较慢。**

**2. Sinking idle ratio较低，那么server端parser是性能瓶颈，可参考[Performance](https://github.com/alibaba/canal/wiki/Performance)进行tuning.**

* Delay(seconds)
```
canal_instance_traffic_delay / 1000
```
**Instance binlog消费延迟，有两个注意点：**

**1. 如果Canal已经消费至最新position，且binlog长时间未更新，delay的resolution受到master_heartbeat_period的影响，目前频率为15秒。**

**2. Limitations: 如果store满了(恰好store中的数据包含最新的position，且MySQL binlog停止更新)，且client暂停消费，那么delay会不断增长。当然，满足这些条件的概率极低。**

* Binlog接收速率(KB/s)
```
rate(canal_instance_received_binlog_bytes{destination="example"}[2m]) / 1024
```
**'Sink线程空闲比'与'Dump线程空闲比'都很低，delay还是很高的情况，请查看binlog接收速率是否符合预期。**


--To be continued...
