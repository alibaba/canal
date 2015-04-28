package com.alibaba.otter.canal.common.zookeeper;

import java.text.MessageFormat;

import org.apache.commons.lang.StringUtils;

/**
 * 存储结构：
 * 
 * <pre>
 * /otter
 *    canal
 *      cluster
 *      destinations
 *        dest1
 *          running (EPHEMERAL) 
 *          cluster
 *          client1
 *            running (EPHEMERAL)
 *            cluster
 *            filter
 *            cursor
 *            mark
 *              1
 *              2
 *              3
 * </pre>
 * 
 * @author zebin.xuzb @ 2012-6-21
 * @version 1.0.0
 */
public class ZookeeperPathUtils {

    public static final String ZOOKEEPER_SEPARATOR                          = "/";

    public static final String OTTER_ROOT_NODE                              = ZOOKEEPER_SEPARATOR + "otter";

    public static final String CANAL_ROOT_NODE                              = OTTER_ROOT_NODE + ZOOKEEPER_SEPARATOR
                                                                              + "canal";

    public static final String DESTINATION_ROOT_NODE                        = CANAL_ROOT_NODE + ZOOKEEPER_SEPARATOR
                                                                              + "destinations";

    public static final String FILTER_NODE                                  = "filter";

    public static final String BATCH_MARK_NODE                              = "mark";

    public static final String PARSE_NODE                                   = "parse";

    public static final String CURSOR_NODE                                  = "cursor";

    public static final String RUNNING_NODE                                 = "running";

    public static final String CLUSTER_NODE                                 = "cluster";

    public static final String DESTINATION_NODE                             = DESTINATION_ROOT_NODE
                                                                              + ZOOKEEPER_SEPARATOR + "{0}";

    public static final String DESTINATION_PARSE_NODE                       = DESTINATION_NODE + ZOOKEEPER_SEPARATOR
                                                                              + PARSE_NODE;

    public static final String DESTINATION_CLIENTID_NODE                    = DESTINATION_NODE + ZOOKEEPER_SEPARATOR
                                                                              + "{1}";

    public static final String DESTINATION_CURSOR_NODE                      = DESTINATION_CLIENTID_NODE
                                                                              + ZOOKEEPER_SEPARATOR + CURSOR_NODE;

    public static final String DESTINATION_CLIENTID_FILTER_NODE             = DESTINATION_CLIENTID_NODE
                                                                              + ZOOKEEPER_SEPARATOR + FILTER_NODE;

    public static final String DESTINATION_CLIENTID_BATCH_MARK_NODE         = DESTINATION_CLIENTID_NODE
                                                                              + ZOOKEEPER_SEPARATOR + BATCH_MARK_NODE;

    public static final String DESTINATION_CLIENTID_BATCH_MARK_WITH_ID_PATH = DESTINATION_CLIENTID_BATCH_MARK_NODE
                                                                              + ZOOKEEPER_SEPARATOR + "{2}";

    /**
     * 服务端当前正在提供服务的running节点
     */
    public static final String DESTINATION_RUNNING_NODE                     = DESTINATION_NODE + ZOOKEEPER_SEPARATOR
                                                                              + RUNNING_NODE;

    /**
     * 客户端当前正在工作的running节点
     */
    public static final String DESTINATION_CLIENTID_RUNNING_NODE            = DESTINATION_CLIENTID_NODE
                                                                              + ZOOKEEPER_SEPARATOR + RUNNING_NODE;

    /**
     * 整个canal server的集群列表
     */
    public static final String CANAL_CLUSTER_ROOT_NODE                      = CANAL_ROOT_NODE + ZOOKEEPER_SEPARATOR
                                                                              + CLUSTER_NODE;

    public static final String CANAL_CLUSTER_NODE                           = CANAL_CLUSTER_ROOT_NODE
                                                                              + ZOOKEEPER_SEPARATOR + "{0}";

    /**
     * 针对某个destination的工作的集群列表
     */
    public static final String DESTINATION_CLUSTER_ROOT                     = DESTINATION_NODE + ZOOKEEPER_SEPARATOR
                                                                              + CLUSTER_NODE;
    public static final String DESTINATION_CLUSTER_NODE                     = DESTINATION_CLUSTER_ROOT
                                                                              + ZOOKEEPER_SEPARATOR + "{1}";

    public static String getDestinationPath(String destinationName) {
        return MessageFormat.format(DESTINATION_NODE, destinationName);
    }

    public static String getClientIdNodePath(String destinationName, short clientId) {
        return MessageFormat.format(DESTINATION_CLIENTID_NODE, destinationName, String.valueOf(clientId));
    }

    public static String getFilterPath(String destinationName, short clientId) {
        return MessageFormat.format(DESTINATION_CLIENTID_FILTER_NODE, destinationName, String.valueOf(clientId));
    }

    public static String getBatchMarkPath(String destinationName, short clientId) {
        return MessageFormat.format(DESTINATION_CLIENTID_BATCH_MARK_NODE, destinationName, String.valueOf(clientId));
    }

    public static String getBatchMarkWithIdPath(String destinationName, short clientId, Long batchId) {
        return MessageFormat.format(DESTINATION_CLIENTID_BATCH_MARK_WITH_ID_PATH,
            destinationName,
            String.valueOf(clientId),
            getBatchMarkNode(batchId));
    }

    public static String getCursorPath(String destination, short clientId) {
        return MessageFormat.format(DESTINATION_CURSOR_NODE, destination, String.valueOf(clientId));
    }

    public static String getCanalClusterNode(String node) {
        return MessageFormat.format(CANAL_CLUSTER_NODE, node);
    }

    /**
     * 服务端当前正在提供服务的running节点
     */
    public static String getDestinationServerRunning(String destination) {
        return MessageFormat.format(DESTINATION_RUNNING_NODE, destination);
    }

    /**
     * 客户端当前正在工作的running节点
     */
    public static String getDestinationClientRunning(String destination, short clientId) {
        return MessageFormat.format(DESTINATION_CLIENTID_RUNNING_NODE, destination, String.valueOf(clientId));
    }

    public static String getDestinationClusterNode(String destination, String node) {
        return MessageFormat.format(DESTINATION_CLUSTER_NODE, destination, node);
    }

    public static String getDestinationClusterRoot(String destination) {
        return MessageFormat.format(DESTINATION_CLUSTER_ROOT, destination);
    }

    public static String getParsePath(String destination) {
        return MessageFormat.format(DESTINATION_PARSE_NODE, destination);
    }

    /**
     * 将batchNode转换为Long
     */
    public static short getClientId(String clientNode) {
        return Short.valueOf(clientNode);
    }

    /**
     * 将batchNode转换为Long
     */
    public static long getBatchMarkId(String batchMarkNode) {
        return Long.valueOf(batchMarkNode);
    }

    /**
     * 将batchId转化为zookeeper中的node名称
     */
    public static String getBatchMarkNode(Long batchId) {
        return StringUtils.leftPad(String.valueOf(batchId.intValue()), 10, '0');
    }
}
