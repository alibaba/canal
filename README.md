<div class="blog_content">
    <div class="iteye-blog-content-contain">

[![Build Status](https://travis-ci.com/alibaba/canal.svg?branch=master)](https://travis-ci.com/alibaba/canal)
![maven](https://img.shields.io/maven-central/v/com.alibaba.otter/canal.svg)
![license](https://img.shields.io/github/license/alibaba/canal.svg)
[![Average time to resolve an issue](http://isitmaintained.com/badge/resolution/alibaba/canal.svg)](http://isitmaintained.com/project/alibaba/canal "Average time to resolve an issue")
[![Percentage of issues still open](http://isitmaintained.com/badge/open/alibaba/canal.svg)](http://isitmaintained.com/project/alibaba/canal "Percentage of issues still open")

<h1>背景</h1>
<p style="font-size: 14px;">   早期，阿里巴巴B2B公司因为存在杭州和美国双机房部署，存在跨机房同步的业务需求。不过早期的数据库同步业务，主要是基于trigger的方式获取增量变更，不过从2010年开始，阿里系公司开始逐步的尝试基于数据库的日志解析，获取增量变更进行同步，由此衍生出了增量订阅&amp;消费的业务，从此开启了一段新纪元。</p>
<p style="font-size: 14px;">   ps. 目前内部版本已经支持mysql和oracle部分版本的日志解析，当前的canal开源版本支持5.7及以下的版本(阿里内部mysql 5.7.13, 5.6.10, mysql 5.5.18和5.1.40/48)</p>
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
<p style="font-size: 14px;">   关键词： mysql binlog parser / real-time / queue&topic </p>
<p style="font-size: 14px;"> </p>
<h2>工作原理</h2>
<h3 style="font-size: 14px;">mysql主备复制实现</h3>
<p><img src="http://dl.iteye.com/upload/attachment/0080/3086/468c1a14-e7ad-3290-9d3d-44ac501a7227.jpg" alt=""><br> 从上层来看，复制分成三步：
<ol>
<li>master将改变记录到二进制日志(binary log)中（这些记录叫做二进制日志事件，binary log events，可以通过show binlog events进行查看）；</li>
<li>slave将master的binary log events拷贝到它的中继日志(relay log)；</li>
<li>slave重做中继日志中的事件，将改变反映它自己的数据。</li>
</ol>
<h3>canal的工作原理：</h3>
<p><img width="590" src="http://dl.iteye.com/upload/attachment/0080/3107/c87b67ba-394c-3086-9577-9db05be04c95.jpg" alt="" height="273">
<p>原理相对比较简单：</p>
<ol>
<li>canal模拟mysql slave的交互协议，伪装自己为mysql slave，向mysql master发送dump协议</li>
<li>mysql master收到dump请求，开始推送binary log给slave(也就是canal)</li>
<li>canal解析binary log对象(原始为byte流)</li>
</ol>

<h1>重要版本更新说明</h1>

canal 1.1.x系列，参考release文档：<a href="https://github.com/alibaba/canal/releases">版本发布信息</a>

1. 整体性能测试&优化,提升了150%. #726 参考: 【[Performance](https://github.com/alibaba/canal/wiki/Performance)】
2. 原生支持prometheus监控 #765 【[Prometheus QuickStart](https://github.com/alibaba/canal/wiki/Prometheus-QuickStart)】
3. 原生支持kafka消息投递 #695 【[Canal Kafka/RocketMQ QuickStart](https://github.com/alibaba/canal/wiki/Canal-Kafka-RocketMQ-QuickStart)】
4. 原生支持aliyun rds的binlog订阅 (解决自动主备切换/oss binlog离线解析) 参考: 【[Aliyun RDS QuickStart](https://github.com/alibaba/canal/wiki/aliyun-RDS-QuickStart)】
5. 原生支持docker镜像 #801 参考:  【[Docker QuickStart](https://github.com/alibaba/canal/wiki/Docker-QuickStart)】

<h1>相关文档</h1>

See the wiki page for : <a href="https://github.com/alibaba/canal/wiki" >wiki文档</a>

<h3><a name="table-of-contents" class="anchor" href="#table-of-contents"><span class="mini-icon mini-icon-link"></span></a>wiki文档列表</h3>
<ul>
<li><a class="internal present" href="https://github.com/alibaba/canal/wiki/Home">Home</a></li>
<li><a class="internal present" href="https://github.com/alibaba/canal/wiki/Introduction">Introduction</a></li>
<li>
<a class="internal present" href="https://github.com/alibaba/canal/wiki/QuickStart">QuickStart</a>
<ul>
<li><a class="internal present" href="https://github.com/alibaba/canal/wiki/Docker-QuickStart">Docker QuickStart</a></li>
<li><a class="internal present" href="https://github.com/alibaba/canal/wiki/Canal-Kafka-RocketMQ-QuickStart">Canal Kafka/RocketMQ QuickStart</a></li>
<li><a class="internal present" href="https://github.com/alibaba/canal/wiki/aliyun-RDS-QuickStart">Aliyun RDS QuickStart</a></li>
<li><a class="internal present" href="https://github.com/alibaba/canal/wiki/Prometheus-QuickStart">Prometheus QuickStart</a></li>
</ul>
</li>
<li><a class="internal present" href="https://github.com/alibaba/canal/wiki/AdminGuide">AdminGuide</a></li>
<li><a class="internal present" href="https://github.com/alibaba/canal/wiki/ClientExample">ClientExample</a></li>
<li><a class="internal present" href="https://github.com/alibaba/canal/wiki/ClientAPI">ClientAPI</a></li>
<li><a class="internal present" href="https://github.com/alibaba/canal/wiki/Performance">Performance</a></li>
<li><a class="internal present" href="https://github.com/alibaba/canal/wiki/DevGuide">DevGuide</a></li>
<li><a class="internal present" href="https://github.com/alibaba/canal/wiki/BinlogChange%28mysql5.6%29">BinlogChange(Mysql5.6)</a></li>
<li><a class="internal present" href="https://github.com/alibaba/canal/wiki/BinlogChange%28MariaDB%29">BinlogChange(MariaDB)</a></li>
<li><a class="internal present" href="https://github.com/alibaba/canal/wiki/TableMetaTSDB">TableMetaTSDB</a></li>
<li><a href="http://alibaba.github.com/canal/release.html">ReleaseNotes</a></li>
<li><a href="https://github.com/alibaba/canal/releases">Download</a></li>
<li><a class="internal present" href="https://github.com/alibaba/canal/wiki/FAQ">FAQ</a></li>
</ul>

<h1>多语言业务</h1>

1. canal整体交互协议设计上使用了protobuf3.0，理论上可以支持绝大部分的多语言场景，欢迎大家提交多客户端的PR
    * canal java客户端: <a href="https://github.com/alibaba/canal/wiki/ClientExample"> https://github.com/alibaba/canal/wiki/ClientExample </a>
    * canal c#客户端开源项目地址：<a href="https://github.com/dotnetcore/CanalSharp"> https://github.com/dotnetcore/CanalSharp </a>
    * canal go客户端开源项目地址：<a href="https://github.com/CanalClient/canal-go"> https://github.com/CanalClient/canal-go </a>
2. canal作为MySQL binlog的增量获取工具，可以将数据投递到MQ系统中，比如Kafka/RocketMQ，可以借助于MQ的多语言能力 
    * 参考文档: [Canal Kafka/RocketMQ QuickStart](https://github.com/alibaba/canal/wiki/Canal-Kafka-RocketMQ-QuickStart)

<h1>相关资料</h1>

* ADC阿里技术嘉年华分享ppt (放在google docs上，可能需要翻墙): <a href="https://docs.google.com/presentation/d/1MkszUPYRDkfVPz9IqOT1LLT5d9tuwde_WC8GZvjaDRg/edit?usp=sharing">ppt下载</href>  
* [与阿里巴巴的RocketMQ配合使用](https://github.com/apache/RocketMQ)

<h1>相关开源</h1>
<ol>
<li>阿里巴巴分布式数据库同步系统(解决中美异地机房)：<a href="http://github.com/alibaba/otter">http://github.com/alibaba/otter</a></li>
<li>阿里巴巴去Oracle数据迁移同步工具(目标支持MySQL/DRDS)：<a href="http://github.com/alibaba/yugong">http://github.com/alibaba/yugong</a></li>
</ol>

<h1>相关产品</h1>
<ol>
<li><a href="https://www.aliyun.com/product/drds?spm=5176.55326.cloudEssentials.71.69fd227dRPZj9K">阿里云分布式数据库DRDS</a></li>
<li><a href="https://www.aliyun.com/product/dts?spm=5176.7947010.cloudEssentials.80.33f734f4JOAxSP">阿里云数据传输服务DTS</a></li>
<li><a href="https://www.aliyun.com/product/dbs?spm=5176.54487.cloudEssentials.83.34b851a8GmVZg6">阿里云数据库备份服务DBS</a></li>
<li><a href="https://www.aliyun.com/product/dms?spm=5176.169464.cloudEssentials.81.2e1066feC1sBBL">阿里云数据管理服务DMS</a></li>
</ol>

<h1>问题反馈</h1>
<ol>
<li>qq交流群： 161559791 </li>
<li>邮件交流： jianghang115@gmail.com </li>
<li>新浪微博： agapple0002 </li>
<li>报告issue：<a href="https://github.com/alibaba/canal/issues">issues</a></li>
</ol>

<h3>最新更新</h3>
<ol>
<li>canal发布重大版本更新1.1.0，具体releaseNode参考：<a href="https://github.com/alibaba/canal/releases/tag/canal-1.1.0">https://github.com/alibaba/canal/releases/tag/canal-1.1.0</a></li>
<li>canal c#客户端开源项目地址：<a href="https://github.com/dotnetcore/CanalSharp"> https://github.com/dotnetcore/CanalSharp </a>，推荐! </li>
<li>canal QQ讨论群已经建立，群号：161559791 ，欢迎加入进行技术讨论。</li>
<li>canal消费端项目开源: Otter(分布式数据库同步系统)，地址：<a href="https://github.com/alibaba/otter">https://github.com/alibaba/otter</a></li>

<li>Canal已在阿里云推出商业化版本 <a href="https://www.aliyun.com/product/dts?spm=a2c4g.11186623.cloudEssentials.80.srdwr7">数据传输服务DTS</a>， 开通即用，免去部署维护的昂贵使用成本。DTS针对阿里云RDS、DRDS等产品进行了适配，解决了Binlog日志回收，主备切换、VPC网络切换等场景下的订阅高可用问题。同时，针对RDS进行了针对性的性能优化。出于稳定性、性能及成本的考虑，强烈推荐阿里云用户使用DTS产品。<a href="https://help.aliyun.com/document_detail/26592.html?spm=a2c4g.11174283.6.539.t1Y91E">DTS产品使用文档</a></li>
DTS支持阿里云RDS&DRDS的Binlog日志实时订阅，现推出首月免费体验，限时限量，<a href="https://common-buy.aliyun.com/?commodityCode=dtspre&request=%7b%22dts_function%22%3a%22data_subscribe%22%7d">立即体验>>></a>
</ol>
