package com.alibaba.otter.canal.distributed.service.zkmonitor;

import java.util.List;

import com.alibaba.otter.canal.distributed.service.MonitorService;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;

import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.common.zookeeper.ZookeeperPathUtils;

/**
 * Zookeeper的监听服务实现
 *
 * @author rewerma 2020-11-8 下午03:07:11
 * @version 1.0.0
 */
public class ZookeeperMonitorService implements MonitorService {

    private ZkClientx zkClientx;
    private String    nodeName;

    public ZookeeperMonitorService(ZkClientx zkClientx){
        this.zkClientx = zkClientx;
    }

    /**
     * 初始化集群节点
     * 
     * @param nodeName 节点名称
     */
    @Override
    public void init(String nodeName) {
        this.nodeName = nodeName;
        final String path = ZookeeperPathUtils.getCanalClusterNode(nodeName);

        // 初始化系统目录
        try {
            zkClientx.createEphemeral(path);
        } catch (ZkNoNodeException e) {
            // 如果父目录不存在，则创建
            // String parentDir = path.substring(0, path.lastIndexOf('/'));
            zkClientx.createPersistent(ZookeeperPathUtils.CANAL_CLUSTER_ROOT_NODE, true);
            zkClientx.createEphemeral(path);
        } catch (ZkNodeExistsException e) {
            // ignore
            // 因为第一次启动时创建了cid,但在stop/start的时可能会关闭和新建,允许出现NodeExists问题s
        }
    }

    /**
     * 监听节点变化
     * 
     * @param listener 监听器回调函数
     * @return 初试返回所有节点
     */
    @Override
    public List<String> listen(Listener listener) {
        return zkClientx.subscribeChildChanges(ZookeeperPathUtils.CANAL_CLUSTER_ROOT_NODE,
            (parentPath, currentChilds) -> listener.listen(currentChilds));
    }

    /**
     * 销毁监听
     */
    @Override
    public void destroy() {
        zkClientx.delete(ZookeeperPathUtils.getCanalClusterNode(nodeName));
    }
}
