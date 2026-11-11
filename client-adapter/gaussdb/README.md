# GaussDB

通过现有 `rdb` 适配器同步到 GaussDB，支持增量同步和全量 ETL。

驱动来源为 [DBeaver GaussDB 插件源码](https://github.com/dbeaver/dbeaver/blob/devel/plugins/org.jkiss.dbeaver.ext.gaussdb/plugin.xml)
声明的 [gsjdbc4-1.1.jar](https://repo.huaweicloud.com/repository/maven/huaweicloudsdk/gsjdbc4/gsjdbc4/1.1/gsjdbc4-1.1.jar)，
构建时从华为仓库下载，无需安装 DBeaver。

原始 GaussDB 驱动与 PostgreSQL 驱动均使用 `org.postgresql` 包。本模块通过 Maven Shade
将 GaussDB 驱动重定位到 `com.alibaba.otter.canal.client.adapter.gaussdb.jdbc`，并同步重写 JDBC
服务声明。原始驱动依赖标记为 optional，防止传递到启动器中。启动器同时携带 PostgreSQL
驱动与重定位后的 GaussDB 驱动，两种数据源可以在同一个 JVM 中使用。

## 构建

在仓库根目录执行：

```bash
mvn -pl client-adapter/launcher -am -Dmaven.test.skip=true package
```

不需要指定数据库构建选项。安装包 `lib` 中应同时包含 `postgresql-*.jar` 和
`client-adapter.gaussdb-*.jar`；不要再加入未重定位的 `gsjdbc4` JAR。

如果 Maven 的全局镜像使用 `mirrorOf: *`，需排除 `huaweicloud-gaussdb`，
或给上述命令增加 `-s client-adapter/rdb/src/test/docker/settings.xml` 使用项目提供的公共仓库配置。

## 配置

`conf/application.yml` 的同一 `outerAdapters` 列表中可以配置：

```yaml
- name: rdb
  key: postgres1
  properties:
    jdbc.driverClassName: org.postgresql.Driver
    jdbc.url: jdbc:postgresql://postgres-host:5432/postgres
    jdbc.username: postgres
    jdbc.password: change_me
- name: rdb
  key: gaussdb1
  properties:
    jdbc.driverClassName: com.alibaba.otter.canal.client.adapter.gaussdb.jdbc.Driver
    jdbc.url: jdbc:postgresql://gaussdb-host:8000/gaussdb?prepareThreshold=0&loginTimeout=20&connectTimeout=20
    jdbc.username: gaussdb
    jdbc.password: change_me
    threads: 1
```

GaussDB 的 JDBC URL 仍使用 `jdbc:postgresql:` 前缀；连接参数与 DBeaver 声明保持一致。
必须显式配置上面的独立驱动类，以区分 PostgreSQL 数据源。

表映射示例位于 `conf/rdb/mytest_user_gaussdb.yml`，`outerAdapterKey: gaussdb1` 对应上述连接，
`targetDb` 填目标 schema（例如 `public`），`targetTable` 单独填表名（例如 `user`），以便分别引用标识符。
按实际源表修改字段映射和数据源配置。
若同时将一张源表同步到 PostgreSQL，为它另建映射文件并设置 `outerAdapterKey: postgres1`。

## Docker 验证

在仓库根目录执行：

```bash
client-adapter/rdb/src/test/docker/test-gaussdb.sh
```

脚本启动独立的 openGauss、PostgreSQL、MariaDB 和 Maven/JDK 8 容器，不占用宿主机数据库端口，
结束时清理本次容器和数据卷。测试覆盖：

- 同一 JVM 中使用各自驱动同时连接 PostgreSQL 和 openGauss。
- 通过 RDB 适配器执行 INSERT、UPDATE、DELETE、TRUNCATE 和计数。
- 中文、单引号、NULL、DECIMAL、BOOLEAN 和保留字表名。
- 从 MySQL 协议源库执行 ETL，验证目标库标识符引用及重复导入。

可通过 `CANAL_GAUSSDB_IMAGE` 指定适合本机架构的 openGauss 镜像，
通过 `CANAL_MAVEN_CACHE` 指定已有 Maven 仓库的绝对路径以复用缓存。
openGauss 容器验证驱动和 SQL 兼容性；商业版 GaussDB 仍需使用实际目标实例验证。
