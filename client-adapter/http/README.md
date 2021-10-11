# http adapter

## `application.yml`

```yml
  canalAdapters:
  - instance: logstore # canal instance Name or mq topic name
    groups:
    - groupId: g1
      outerAdapters:
      - name: http
        key: user_http
        properties:
          serviceUrl: http://127.0.0.1/sync
          sign: 123
```

## adapter config

```yml
dataSourceKey: defaultDS
destination: logstore
groupId: g1
outerAdapterKey: user_http

httpMapping:
  etlSetting:
    database: logstore
    table: user
    condition: "where userId >= {}"
    batchSize: 3000
    threads: 16
  
  monitorTables:
    - tableName: logstore.user

    - tableName: logstore.role
      actions:
        - INSERT

    - tableName: logstore.account
      actions:
        - DELETE

```
