package com.alibaba.otter.canal.client;

import java.util.concurrent.TimeUnit;

import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;

/**
 * canal数据操作客户端
 * 
 * @author zebin.xuzb @ 2012-6-19
 * @author jianghang
 * @version 1.0.0
 */
public interface CanalConnector {

    /**
     * 链接对应的canal server
     * 
     * @throws CanalClientException
     */
    void connect() throws CanalClientException;

    /**
     * 释放链接
     * 
     * @throws CanalClientException
     */
    void disconnect() throws CanalClientException;

    /**
     * 检查下链接是否合法
     * 
     * <pre>
     * 几种case下链接不合法:
     * 1. 链接canal server失败，一直没有一个可用的链接，返回false
     * 2. 当前客户端在进行running抢占的时候，做为备份节点存在，非处于工作节点，返回false
     * 
     * 说明：
     * a. 当前客户端一旦做为备份节点存在，当前所有的对{@linkplain CanalConnector}的操作都会处于阻塞状态，直到转为工作节点
     * b. 所以业务方最好定时调用checkValid()方法用，比如调用CanalConnector所在线程的interrupt，直接退出CanalConnector，并根据自己的需要退出自己的资源
     * </pre>
     * 
     * @throws CanalClientException
     */
    boolean checkValid() throws CanalClientException;

    /**
     * 客户端订阅，重复订阅时会更新对应的filter信息
     * 
     * <pre>
     * 说明：
     * a. 如果本次订阅中filter信息为空，则直接使用canal server服务端配置的filter信息
     * b. 如果本次订阅中filter信息不为空，目前会直接替换canal server服务端配置的filter信息，以本次提交的为准
     * 
     * TODO: 后续可以考虑，如果本次提交的filter不为空，在执行过滤时，是对canal server filter + 本次filter的交集处理，达到只取1份binlog数据，多个客户端消费不同的表
     * </pre>
     * 
     * @throws CanalClientException
     */
    void subscribe(String filter) throws CanalClientException;

    /**
     * 客户端订阅，不提交客户端filter，以服务端的filter为准
     * 
     * @throws CanalClientException
     */
    void subscribe() throws CanalClientException;

    /**
     * 取消订阅
     * 
     * @throws CanalClientException
     */
    void unsubscribe() throws CanalClientException;

    /**
     * 获取数据，自动进行确认，该方法返回的条件：尝试拿batchSize条记录，有多少取多少，不会阻塞等待
     * 
     * @param batchSize
     * @return
     * @throws CanalClientException
     */
    Message get(int batchSize) throws CanalClientException;

    /**
     * 获取数据，自动进行确认
     * 
     * <pre>
     * 该方法返回的条件：
     *  a. 拿够batchSize条记录或者超过timeout时间
     *  b. 如果timeout=0，则阻塞至拿到batchSize记录才返回
     * </pre>
     * 
     * @param batchSize
     * @return
     * @throws CanalClientException
     */
    Message get(int batchSize, Long timeout, TimeUnit unit) throws CanalClientException;

    /**
     * 不指定 position 获取事件，该方法返回的条件: 尝试拿batchSize条记录，有多少取多少，不会阻塞等待<br/>
     * canal 会记住此 client 最新的position。 <br/>
     * 如果是第一次 fetch，则会从 canal 中保存的最老一条数据开始输出。
     * 
     * @param batchSize
     * @throws CanalClientException
     */
    Message getWithoutAck(int batchSize) throws CanalClientException;

    /**
     * 不指定 position 获取事件.
     * 
     * <pre>
     * 该方法返回的条件：
     *  a. 拿够batchSize条记录或者超过timeout时间
     *  b. 如果timeout=0，则阻塞至拿到batchSize记录才返回
     * </pre>
     * 
     * canal 会记住此 client 最新的position。 <br/>
     * 如果是第一次 fetch，则会从 canal 中保存的最老一条数据开始输出。
     * 
     * @param batchSize
     * @param timeout
     * @param unit
     * @return
     * @throws CanalClientException
     */
    Message getWithoutAck(int batchSize, Long timeout, TimeUnit unit) throws CanalClientException;

    /**
     * 进行 batch id 的确认。确认之后，小于等于此 batchId 的 Message 都会被确认。
     * 
     * @param batchId
     * @throws CanalClientException
     */
    void ack(long batchId) throws CanalClientException;

    /**
     * 回滚到未进行 {@link #ack} 的地方，指定回滚具体的batchId
     * 
     * @throws CanalClientException
     */
    void rollback(long batchId) throws CanalClientException;

    /**
     * 回滚到未进行 {@link #ack} 的地方，下次fetch的时候，可以从最后一个没有 {@link #ack} 的地方开始拿
     * 
     * @throws CanalClientException
     */
    void rollback() throws CanalClientException;
}
