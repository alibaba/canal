<div class="blog_content">
    <div class="iteye-blog-content-contain">
<p style="font-size: 14px;">  </p>
<h3>最新更新：cancal QQ讨论群已经建立，群号：161559791 ，欢迎加入进行技术讨论。</h3>
<h1>背景</h1>
<p style="font-size: 14px;">   早期，阿里巴巴B2B公司因为存在杭州和美国双机房部署，存在跨机房同步的业务需求。不过早期的数据库同步业务，主要是基于trigger的方式获取增量变更，不过从2010年开始，阿里系公司开始逐步的尝试基于数据库的日志解析，获取增量变更进行同步，由此衍生出了增量订阅&amp;消费的业务，从此开启了一段新纪元。</p>
    <p style="font-size: 14px;">   ps. 目前内部版本已经支持mysql和oracle部分版本的日志解析，当前的canal开源版本支持5.5及以下的版本(5.6暂时不支持，阿里内部mysql 5.5.18和5.1.40/48)</p>
<p style="font-size: 14px;"> </p>
<p style="font-size: 14px;">基于日志增量订阅&amp;消费支持的业务：</p>
<ol style="font-size: 14px;">
<li>数据库镜像</li>
<li>数据库实时备份</li>
<li>多级索引 (卖家和买家各自分库索引)</li>
<li>search build</li>
<li>业务cache刷新</li>
<li>价格变化等重要业务消息</li>
</ol>
<h1>项目介绍</h1>
<p style="font-size: 14px;">   名称：canal [kə'næl]</p>
<p style="font-size: 14px;">   译意： 水道/管道/沟渠 </p>
<p style="font-size: 14px;">   语言： 纯java开发</p>
<p style="font-size: 14px;">   定位： 基于数据库增量日志解析，提供增量数据订阅&amp;消费，目前主要支持了mysql</p>
<p style="font-size: 14px;"> </p>
<h2>工作原理</h2>
<h3 style="font-size: 14px;">mysql主备复制实现</h3>
<p><img src="http://dl.iteye.com/upload/attachment/0080/3086/468c1a14-e7ad-3290-9d3d-44ac501a7227.jpg" alt=""><br> 从上层来看，复制分成三步：</p>
<ol>
<li>master将改变记录到二进制日志(binary log)中（这些记录叫做二进制日志事件，binary log events，可以通过show binlog events进行查看）；</li>
<li>slave将master的binary log events拷贝到它的中继日志(relay log)；</li>
<li>slave重做中继日志中的事件，将改变反映它自己的数据。</li>
</ol>
<h3>canal的工作原理：</h3>
<p><img width="590" src="http://dl.iteye.com/upload/attachment/0080/3107/c87b67ba-394c-3086-9577-9db05be04c95.jpg" alt="" height="273"></p>
<p>原理相对比较简单：</p>
<ol>
<li>canal模拟mysql slave的交互协议，伪装自己为mysql slave，向mysql master发送dump协议</li>
<li>mysql master收到dump请求，开始推送binary log给slave(也就是canal)</li>
<li>canal解析binary log对象(原始为byte流)</li>
</ol>
<h1>架构</h1>
<p><img width="548" src="http://dl.iteye.com/upload/attachment/0080/3126/49550085-0cd2-32fa-86a6-f676db5b597b.jpg" alt="" height="238" style="line-height: 1.5;"></p>
<p style="color: #333333; background-image: none; margin-top: 10px; margin-bottom: 10px; font-family: Arial, Helvetica, FreeSans, sans-serif;">说明：</p>
<ul style="line-height: 1.5; color: #333333; font-family: Arial, Helvetica, FreeSans, sans-serif;">
<li>server代表一个canal运行实例，对应于一个jvm</li>
<li>instance对应于一个数据队列  （1个server对应1..n个instance)</li>
</ul>
<p>instance模块：</p>
<ul style="line-height: 1.5; color: #333333; font-family: Arial, Helvetica, FreeSans, sans-serif;">
<li>eventParser (数据源接入，模拟slave协议和master进行交互，协议解析)</li>
<li>eventSink (Parser和Store链接器，进行数据过滤，加工，分发的工作)</li>
<li>eventStore (数据存储)</li>
<li>metaManager (增量订阅&amp;消费信息管理器)</li>
</ul>
<h3>数据对象格式：<a href="https://github.com/otter-projects/canal/blob/master/protocol/src/main/java/com/alibaba/otter/canal/protocol/EntryProtocol.proto" style="font-size: 14px; line-height: 1.5; color: #bc2a4d; text-decoration: underline;">EntryProtocol.proto</a>
</h3>
<pre name="code" class="java">Entry
    Header
		logfileName [binlog文件名]
		logfileOffset [binlog position]
		executeTime [发生的变更]
		schemaName 
		tableName
		eventType [insert/update/delete类型]
	entryType 	[事务头BEGIN/事务尾END/数据ROWDATA]
	storeValue 	[byte数据,可展开，对应的类型为RowChange]
	
RowChange
	isDdl		[是否是ddl变更操作，比如create table/drop table]
	sql		[具体的ddl sql]
	rowDatas	[具体insert/update/delete的变更数据，可为多条，1个binlog event事件可对应多条变更，比如批处理]
		beforeColumns [Column类型的数组]
		afterColumns [Column类型的数组]
		
Column 
	index		
	sqlType		[jdbc type]
	name		[column name]
	isKey		[是否为主键]
	updated		[是否发生过变更]
	isNull		[值是否为null]
	value		[具体的内容，注意为文本]</pre>
<p>说明：</p>
<ul>
<li>可以提供数据库变更前和变更后的字段内容，针对binlog中没有的name,isKey等信息进行补全</li>
<li>可以提供ddl的变更语句</li>
</ul>
<h1>QuickStart</h1>
<h2>几点说明：(mysql初始化)</h2>
<p>a.  canal的原理是基于mysql binlog技术，所以这里一定需要开启mysql的binlog写入功能，并且配置binlog模式为row. </p>
<pre class="java" name="code">[mysqld]
log-bin=mysql-bin #添加这一行就ok
binlog-format=ROW #选择row模式
server_id=1 #配置mysql replaction需要定义，不能和canal的slaveId重复</pre>
b.  canal的原理是模拟自己为mysql slave，所以这里一定需要做为mysql slave的相关权限.</div>
<div class="iteye-blog-content-contain">
<pre class="java" name="code">CREATE USER canal IDENTIFIED BY 'canal';  
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'canal'@'%';
-- GRANT ALL PRIVILEGES ON *.* TO 'canal'@'%' ;
FLUSH PRIVILEGES;</pre>
<p>针对已有的账户可通过grant
<h2>启动步骤：</h2>
<p>1.  下载canal</p>
<p>下载部署包</p>
<pre name="code" class="java">wget http://canal4mysql.googlecode.com/files/canal.deployer-1.0.1.tar.gz</pre>
<p>or </p>
<p>自己编译 </p>
<pre name="code" class="java">git clone git@github.com:otter-projects/canal.git
cd canal; 
mvn clean install -Dmaven.test.skip -Denv=release</pre>
<p>    编译完成后，会在根目录下产生target/canal.deployer-$version.tar.gz </p>
<p> </p>
<p>2.  解压缩</p>
<pre name="code" class="java">mkdir /tmp/canal
tar zxvf canal.deployer-$version.tar.gz  -C /tmp/canal</pre>
<p>   </p>
<p>   解压完成后，进入/tmp/canal目录，可以看到如下结构：</p>
<p> </p>
<pre name="code" class="java">drwxr-xr-x 2 jianghang jianghang  136 2013-02-05 21:51 bin
drwxr-xr-x 4 jianghang jianghang  160 2013-02-05 21:51 conf
drwxr-xr-x 2 jianghang jianghang 1.3K 2013-02-05 21:51 lib
drwxr-xr-x 2 jianghang jianghang   48 2013-02-05 21:29 logs</pre>
<p> </p>
<p>3.  配置修改</p>
<p> </p>
<p>应用参数：</p>
<pre name="code" class="shell">vi conf/example/instance.properties</pre>
<pre name="code" class="instance.properties">#################################################
## mysql serverId
canal.instance.mysql.slaveId = 1234

# position info
canal.instance.master.address = 127.0.0.1:3306 #改成自己的数据库地址
canal.instance.master.journal.name = 
canal.instance.master.position = 
canal.instance.master.timestamp = 

#canal.instance.standby.address = 
#canal.instance.standby.journal.name =
#canal.instance.standby.position = 
#canal.instance.standby.timestamp = 

# username/password
canal.instance.dbUsername = canal  #改成自己的数据库信息
canal.instance.dbPassword = canal  #改成自己的数据库信息
canal.instance.defaultDatabaseName =   #改成自己的数据库信息
canal.instance.connectionCharset = UTF-8  #改成自己的数据库信息

# table regex
canal.instance.filter.regex = .*\\..*

#################################################
</pre>
<p> </p>
<p> </p>
<p> 说明：</p>
<ul>
<li>canal.instance.connectionCharset 代表数据库的编码方式对应到java中的编码类型，比如UTF-8，GBK , ISO-8859-1</li>
</ul>
<p>4.   准备启动</p>
<p> </p>
<pre name="code" class="java">sh bin/startup.sh</pre>
<p> </p>
<p>5.  查看日志</p>
<pre name="code" class="java">vi logs/canal/canal.log</pre>
<pre name="code" class="java">2013-02-05 22:45:27.967 [main] INFO  com.alibaba.otter.canal.deployer.CanalLauncher - ## start the canal server.
2013-02-05 22:45:28.113 [main] INFO  com.alibaba.otter.canal.deployer.CanalController - ## start the canal server[10.1.29.120:11111]
2013-02-05 22:45:28.210 [main] INFO  com.alibaba.otter.canal.deployer.CanalLauncher - ## the canal server is running now ......</pre>
<p>     </p>
<p>    具体instance的日志：</p>
<pre name="code" class="java">vi logs/example/example.log</pre>
<pre name="code" class="java">2013-02-05 22:50:45.636 [main] INFO  c.a.o.c.i.spring.support.PropertyPlaceholderConfigurer - Loading properties file from class path resource [canal.properties]
2013-02-05 22:50:45.641 [main] INFO  c.a.o.c.i.spring.support.PropertyPlaceholderConfigurer - Loading properties file from class path resource [example/instance.properties]
2013-02-05 22:50:45.803 [main] INFO  c.a.otter.canal.instance.spring.CanalInstanceWithSpring - start CannalInstance for 1-example 
2013-02-05 22:50:45.810 [main] INFO  c.a.otter.canal.instance.spring.CanalInstanceWithSpring - start successful....</pre>
<p> </p>
<p>6.  关闭</p>
<pre name="code" class="java">sh bin/stop.sh</pre>
<p> </p>
<p>it's over. </p>
</div>
<h1>ClientExample</h1>
<p>依赖配置：(目前暂未正式发布到mvn仓库，所以需要各位下载canal源码后手工执行下mvn clean install -Dmaven.test.skip)</p>
<pre name="code" class="java">&lt;dependency&gt;
    &lt;groupId&gt;com.alibaba.otter&lt;/groupId&gt;
    &lt;artifactId&gt;canal.client&lt;/artifactId&gt;
    &lt;version&gt;1.0.0&lt;/version&gt;
&lt;/dependency&gt;</pre>
<p> </p>
<p>1. 创建mvn标准工程：</p>
<pre name="code" class="java">mvn archetype:create -DgroupId=com.alibaba.otter -DartifactId=canal.sample</pre>
<p> </p>
<p>2.  修改pom.xml，添加依赖</p>
<p> </p>
<p>3.  ClientSample代码</p>
<pre name="code" class="SimpleCanalClientExample">package com.alibaba.otter.canal.sample;

import java.net.InetSocketAddress;
import java.util.List;

import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;

public class SimpleCanalClientExample {

    public static void main(String args[]) {
        // 创建链接
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(AddressUtils.getHostIp(),
                                                                                            11111), "example", "", "");
        int batchSize = 1000;
        int emptyCount = 0;
        try {
            connector.connect();
            connector.subscribe(".*\\..*");
            connector.rollback();
            int totalEmtryCount = 120;
            while (emptyCount &lt; totalEmtryCount) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    emptyCount++;
                    System.out.println("empty count : " + emptyCount);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                } else {
                    emptyCount = 0;
                    // System.out.printf("message[batchId=%s,size=%s] \n", batchId, size);
                    printEntry(message.getEntries());
                }

                connector.ack(batchId); // 提交确认
                // connector.rollback(batchId); // 处理失败, 回滚数据
            }

            System.out.println("empty too many times, exit");
        } finally {
            connector.disconnect();
        }
    }

    private static void printEntry(List&lt;Entry&gt; entrys) {
        for (Entry entry : entrys) {
            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }

            RowChange rowChage = null;
            try {
                rowChage = RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                                           e);
            }

            EventType eventType = rowChage.getEventType();
            System.out.println(String.format("================&gt; binlog[%s:%s] , name[%s,%s] , eventType : %s",
                                             entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                                             entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                                             eventType));

            for (RowData rowData : rowChage.getRowDatasList()) {
                if (eventType == EventType.DELETE) {
                    printColumn(rowData.getBeforeColumnsList());
                } else if (eventType == EventType.INSERT) {
                    printColumn(rowData.getAfterColumnsList());
                } else {
                    System.out.println("-------&gt; before");
                    printColumn(rowData.getBeforeColumnsList());
                    System.out.println("-------&gt; after");
                    printColumn(rowData.getAfterColumnsList());
                }
            }
        }
    }

    private static void printColumn(List&lt;Column&gt; columns) {
        for (Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
    }
}</pre>
<p> </p>
<p>4. 运行Client</p>
<p>首先启动Canal Server，可参加QuickStart : <a style="line-height: 1.5;" href="/blogs/1796070">http://agapple.iteye.com/blogs/1796070</a></p>
<p>启动Canal Client后，可以从控制台从看到类似消息：</p>
<pre name="code" class="java">empty count : 1
empty count : 2
empty count : 3
empty count : 4</pre>
<p> 此时代表当前数据库无变更数据</p>
<p> </p>
<p>5.  触发数据库变更</p>
<pre name="code" class="java">mysql&gt; use test;
Database changed
mysql&gt; CREATE TABLE `xdual` (
    -&gt;   `ID` int(11) NOT NULL AUTO_INCREMENT,
    -&gt;   `X` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -&gt;   PRIMARY KEY (`ID`)
    -&gt; ) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8 ;
Query OK, 0 rows affected (0.06 sec)

mysql&gt; insert into xdual(id,x) values(null,now());Query OK, 1 row affected (0.06 sec)</pre>
<p> </p>
<p>可以从控制台中看到：</p>
<pre name="code" class="java">empty count : 1
empty count : 2
empty count : 3
empty count : 4
================&gt; binlog[mysql-bin.001946:313661577] , name[test,xdual] , eventType : INSERT
ID : 4    update=true
X : 2013-02-05 23:29:46    update=true</pre>
<p> </p>
<h2>最后：</h2>
<p>  整个代码在附件中可以下载，如有问题可及时联系。 </p>
</div>
    
  <div class="attachments">
<a href="http://dl.iteye.com/topics/download/7a893f19-bafb-313a-8a7a-e371a4265ad9">canal.sample.tar.gz</a> (2.2 KB)
  </div>
