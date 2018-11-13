## RDB适配器
RDB adapter 用于适配mysql到任意关系型数据库(需支持jdbc)的数据同步及导入
### 1.1 修改启动器配置: application.yml, 这里以oracle目标库为例
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
  srcDataSources:
    defaultDS:
      url: jdbc:mysql://127.0.0.1:3306/mytest?useUnicode=true
      username: root
      password: 121212
  canalInstances:
  - instance: example
    groups:
    - outAdapters:
      - name: rdb
        key: oracle1
        properties:
          jdbc.driverClassName: oracle.jdbc.OracleDriver
          jdbc.url: jdbc:oracle:thin:@localhost:49161:XE
          jdbc.username: mytest
          jdbc.password: m121212
```
其中 outAdapter 的配置: name统一为rdb, key为对应的数据源的唯一标识需和下面的表映射文件中的outerAdapterKey对应!! properties为目标库jdb的相关参数
adapter将会自动加载 conf/rdb 下的所有.yml结尾的表映射配置文件
### 1.2 适配器表映射文件
修改 conf/rdb/mytest_user.yml文件:
```
dataSourceKey: defaultDS        # 源数据源的key, 对应上面配置的srcDataSources中的值
destination: example            # cannal的instance或者MQ的topic
outerAdapterKey: oracle1        # adapter key, 对应上面配置outAdapters中的key
dbMapping:
  database: mytest              # 源数据源的database/shcema
  table: user                   # 源数据源表名
  targetTable: mytest.tb_user   # 目标数据源的库名.表名
  targetPk:                     # 主键映射
    id: id                      # 如果是复合主键可以换行映射多个
  mapAll: true                  # 是否整表映射, 要求源表和目标表字段名一模一样 (如果targetColumns也配置了映射,则以targetColumns配置为准)
#  targetColumns:               # 字段映射, 格式: 目标表字段: 源表字段, 如果字段名一样源表字段名可不填
#    id:
#    name:
#    role_id:
#    c_time:
#    test1: 
```
导入的类型以目标表的元类型为准, 将自动转换

### 1.3 启动RDB数据同步
#### 将目标库的jdbc jar包放入lib文件夹, 这里放入ojdbc6.jar

#### 启动canal-adapter启动器
```
bin/startup.sh
```
#### 验证
修改mysql mytest.user表的数据, 将会自动同步到Oracle的MYTEST.TB_USER表下面, 并会打出DML的log
