package com.alibaba.otter.canal.client;

import java.net.SocketAddress;

/**
 * 集群节点访问控制接口
 * 
 * @author jianghang 2012-10-29 下午07:55:41
 * @version 4.1.2
 */
public interface CanalNodeAccessStrategy {

    SocketAddress nextNode();
}
