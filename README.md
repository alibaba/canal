<div class="blog_content">
    <div class="iteye-blog-content-contain">

<h3>最新更新</h3>
<ol>
<li>canal QQ讨论群已经建立，群号：161559791 ，欢迎加入进行技术讨论。</li>
<li>canal消费端项目开源: Otter(分布式数据库同步系统)，地址：<a href="https://github.com/alibaba/otter">https://github.com/alibaba/otter</a></li>
</ol>

<h1>背景</h1>
<p style="font-size: 14px;">   早期，阿里巴巴B2B公司因为存在杭州和美国双机房部署，存在跨机房同步的业务需求。不过早期的数据库同步业务，主要是基于trigger的方式获取增量变更，不过从2010年开始，阿里系公司开始逐步的尝试基于数据库的日志解析，获取增量变更进行同步，由此衍生出了增量订阅&amp;消费的业务，从此开启了一段新纪元。</p>
<p style="font-size: 14px;">   ps. 目前内部版本已经支持mysql和oracle部分版本的日志解析，当前的canal开源版本支持5.6及以下的版本(阿里内部mysql 5.6.10, mysql 5.5.18和5.1.40/48)</p>
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

<h1>相关文档</h1>

See the wiki page for : <a href="https://github.com/alibaba/canal/wiki" >wiki文档</a>

<h3><a name="table-of-contents" class="anchor" href="#table-of-contents"><span class="mini-icon mini-icon-link"></span></a>wiki文档列表</h3>
<ul>
<li><a class="internal present" href="https://github.com/alibaba/canal/wiki/Home">Home</a></li>
<li><a class="internal present" href="https://github.com/alibaba/canal/wiki/Introduction">Introduction</a></li>
<li><a class="internal present" href="https://github.com/alibaba/canal/wiki/QuickStart">QuickStart</a></li>
<li><a class="internal present" href="https://github.com/alibaba/canal/wiki/ClientExample">ClientExample</a></li>
<li><a class="internal present" href="https://github.com/alibaba/canal/wiki/AdminGuide">AdminGuide</a></li>
<li><a class="internal present" href="https://github.com/alibaba/canal/wiki/ClientAPI">ClientAPI</a></li>
<li><a class="internal present" href="https://github.com/alibaba/canal/wiki/DevGuide">DevGuide</a></li>
<li><a class="internal present" href="https://github.com/alibaba/canal/wiki/BinlogChange%28mysql5.6%29">BinlogChange(Mysql5.6)</a></li>
<li><a href="http://alibaba.github.com/canal/release.html">ReleaseNotes</a></li>
<li><a href="https://github.com/alibaba/canal/releases">Download</a></li>
</ul>

<h1>问题反馈</h1>
<ol>
<li>qq交流群： 161559791 </li>
<li>邮件交流： jianghang115@gmail.com </li>
<li>新浪微博： agapple0002 </li>
<li>报告issue：<a href="https://github.com/alibaba/canal/issues">issues</a></li>
</ol>
