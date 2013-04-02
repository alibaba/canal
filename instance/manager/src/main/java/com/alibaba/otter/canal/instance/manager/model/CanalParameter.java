package com.alibaba.otter.canal.instance.manager.model;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.alibaba.otter.canal.common.utils.CanalToStringStyle;
import com.alibaba.otter.canal.store.model.BatchMode;

/**
 * canal运行相关参数
 * 
 * @author jianghang 2012-7-4 下午02:52:52
 * @version 1.0.0
 */
public class CanalParameter implements Serializable {

    private static final long        serialVersionUID                   = -5893459662315430900L;
    private Long                     canalId;

    // 相关参数
    private RunMode                  runMode                            = RunMode.EMBEDDED;          // 运行模式：嵌入式/服务式
    private List<String>             zkClusters;                                                     // zk集群地址

    // meta相关参数
    private MetaMode                 metaMode                           = MetaMode.MEMORY;           // meta机制

    // storage存储
    private Integer                  transactionSize                    = 1024;                      // 支持处理的transaction事务大小
    private StorageMode              storageMode                        = StorageMode.MEMORY;        // 存储机制
    private BatchMode                storageBatchMode                   = BatchMode.MEMSIZE;         // 基于大小返回结果
    private Integer                  memoryStorageBufferSize            = 16 * 1024;                 // 内存存储的buffer大小
    private Integer                  memoryStorageBufferMemUnit         = 1024;                      // 内存存储的buffer内存占用单位，默认为1kb
    private String                   fileStorageDirectory;                                           // 文件存储的目录位置
    private Integer                  fileStorageStoreCount;                                          // 每个文件store存储的记录数
    private Integer                  fileStorageRollverCount;                                        // store文件的个数
    private Integer                  fileStoragePercentThresold;                                     // 整个store存储占disk硬盘的百分比，超过百分比及时条数还未满也不写入
    private StorageScavengeMode      storageScavengeMode                = StorageScavengeMode.ON_ACK;
    private String                   scavengeSchdule;                                                // 调度规则

    // replcation相关参数
    private SourcingType             sourcingType                       = SourcingType.MYSQL;        // 数据来源类型
    private String                   localBinlogDirectory;                                           // 本地localBinlog目录
    private HAMode                   haMode                             = HAMode.HEARTBEAT;          // ha机制
    // 网络链接参数
    private Integer                  port                               = 11111;                     // 服务端口，独立运行时需要配置
    private Integer                  defaultConnectionTimeoutInSeconds  = 30;                        // sotimeout
    private Integer                  receiveBufferSize                  = 64 * 1024;
    private Integer                  sendBufferSize                     = 64 * 1024;
    // 编码信息
    private Byte                     connectionCharsetNumber            = (byte) 33;
    private String                   connectionCharset                  = "UTF-8";

    // 数据库信息
    private List<InetSocketAddress>  dbAddresses;                                                    // 数据库链接信息
    private List<List<DataSourcing>> groupDbAddresses;                                               // 数据库链接信息，包含多组信息
    private String                   dbUsername;                                                     // 数据库用户
    private String                   dbPassword;                                                     // 数据库密码

    // binlog链接信息
    private IndexMode                indexMode;
    private List<String>             positions;                                                      // 数据库positions信息
    private String                   defaultDatabaseName;                                            // 默认链接的数据库schmea
    private Long                     slaveId;                                                        // 链接到mysql的slaveId
    private Integer                  fallbackIntervalInSeconds          = 60;                        // 数据库发生切换查找时回退的时间

    // 心跳检查信息
    private Boolean                  detectingEnable                    = true;                      // 是否开启心跳语句
    private Boolean                  heartbeatHaEnable                  = false;                     // 是否开启基于心跳检查的ha功能
    private String                   detectingSQL;                                                   // 心跳sql
    private Integer                  detectingIntervalInSeconds         = 3;                         // 检测频率
    private Integer                  detectingTimeoutThresholdInSeconds = 30;                        // 心跳超时时间
    private Integer                  detectingRetryTimes                = 3;                         // 心跳检查重试次数

    // tddl/diamond 配置信息
    private String                   app;
    private String                   group;
    // metaq 存储配置信息
    private String                   metaqStoreUri;

    // ================================== 兼容字段处理
    private InetSocketAddress        masterAddress;                                                  // 主库信息
    private String                   masterUsername;                                                 // 帐号
    private String                   masterPassword;                                                 // 密码

    private InetSocketAddress        standbyAddress;                                                 // 备库信息
    private String                   standbyUsername;                                                // 帐号
    private String                   standbyPassword;
    private String                   masterLogfileName                  = null;                      // master起始位置
    private Long                     masterLogfileOffest                = null;
    private Long                     masterTimestamp                    = null;
    private String                   standbyLogfileName                 = null;                      // standby起始位置
    private Long                     standbyLogfileOffest               = null;
    private Long                     standbyTimestamp                   = null;

    public static enum RunMode {

        /** 嵌入式 */
        EMBEDDED,
        /** 服务式 */
        SERVICE;

        public boolean isEmbedded() {
            return this.equals(RunMode.EMBEDDED);
        }

        public boolean isService() {
            return this.equals(RunMode.SERVICE);
        }
    }

    public static enum HAMode {
        /** cobar ha控制 */
        COBAR,
        /** tddl ha控制 */
        TDDL,
        /** 心跳检测 */
        HEARTBEAT;

        public boolean isCobar() {
            return this.equals(HAMode.COBAR);
        }

        public boolean isTddl() {
            return this.equals(HAMode.TDDL);
        }

        public boolean isHeartBeat() {
            return this.equals(HAMode.HEARTBEAT);
        }
    }

    public static enum StorageMode {
        /** 内存存储模式 */
        MEMORY,
        /** 文件存储模式 */
        FILE,
        /** 混合模式，内存+文件 */
        MIXED,
        /** metaq模式 */
        METAQ;

        public boolean isMemory() {
            return this.equals(StorageMode.MEMORY);
        }

        public boolean isFile() {
            return this.equals(StorageMode.FILE);
        }

        public boolean isMixed() {
            return this.equals(StorageMode.MIXED);
        }

        public boolean isMetaq() {
            return this.equals(StorageMode.METAQ);
        }
    }

    public static enum StorageScavengeMode {
        /** 在存储满的时候触发 */
        ON_FULL,
        /** 在每次有ack请求时触发 */
        ON_ACK,
        /** 定时触发，需要外部控制 */
        ON_SCHEDULE,
        /** 不做任何操作，由外部进行清理 */
        NO_OP;

        public boolean isOnFull() {
            return this.equals(StorageScavengeMode.ON_FULL);
        }

        public boolean isOnAck() {
            return this.equals(StorageScavengeMode.ON_ACK);
        }

        public boolean isOnSchedule() {
            return this.equals(StorageScavengeMode.ON_SCHEDULE);
        }

        public boolean isNoop() {
            return this.equals(StorageScavengeMode.NO_OP);
        }
    }

    public static enum SourcingType {
        /** mysql DB */
        MYSQL,
        /** localBinLog */
        LOCALBINLOG,
        /** oracle DB */
        ORACLE,
        /** 多库合并模式 */
        GROUP;

        public boolean isMysql() {
            return this.equals(SourcingType.MYSQL);
        }

        public boolean isLocalBinlog() {
            return this.equals(SourcingType.LOCALBINLOG);
        }

        public boolean isOracle() {
            return this.equals(SourcingType.ORACLE);
        }

        public boolean isGroup() {
            return this.equals(SourcingType.GROUP);
        }
    }

    public static enum MetaMode {
        /** 内存存储模式 */
        MEMORY,
        /** 文件存储模式 */
        ZOOKEEPER,
        /** 混合模式，内存+文件 */
        MIXED;

        public boolean isMemory() {
            return this.equals(MetaMode.MEMORY);
        }

        public boolean isZookeeper() {
            return this.equals(MetaMode.ZOOKEEPER);
        }

        public boolean isMixed() {
            return this.equals(MetaMode.MIXED);
        }
    }

    public static enum IndexMode {
        /** 内存存储模式 */
        MEMORY,
        /** 文件存储模式 */
        ZOOKEEPER,
        /** 混合模式，内存+文件 */
        MIXED,
        /** 基于meta信息 */
        META,
        /** 基于内存+meta的failback实现 */
        MEMORY_META_FAILBACK;

        public boolean isMemory() {
            return this.equals(IndexMode.MEMORY);
        }

        public boolean isZookeeper() {
            return this.equals(IndexMode.ZOOKEEPER);
        }

        public boolean isMixed() {
            return this.equals(IndexMode.MIXED);
        }

        public boolean isMeta() {
            return this.equals(IndexMode.META);
        }

        public boolean isMemoryMetaFailback() {
            return this.equals(IndexMode.MEMORY_META_FAILBACK);
        }
    }

    /**
     * 数据来源描述
     * 
     * @author jianghang 2012-12-26 上午11:05:20
     * @version 4.1.5
     */
    public static class DataSourcing implements Serializable {

        private static final long serialVersionUID = -1770648468678085234L;
        private SourcingType      type;
        private InetSocketAddress dbAddress;

        public DataSourcing(){

        }

        public DataSourcing(SourcingType type, InetSocketAddress dbAddress){
            this.type = type;
            this.dbAddress = dbAddress;
        }

        public SourcingType getType() {
            return type;
        }

        public void setType(SourcingType type) {
            this.type = type;
        }

        public InetSocketAddress getDbAddress() {
            return dbAddress;
        }

        public void setDbAddress(InetSocketAddress dbAddress) {
            this.dbAddress = dbAddress;
        }

    }

    public Long getCanalId() {
        return canalId;
    }

    public void setCanalId(Long canalId) {
        this.canalId = canalId;
    }

    public RunMode getRunMode() {
        return runMode;
    }

    public void setRunMode(RunMode runMode) {
        this.runMode = runMode;
    }

    public List<String> getZkClusters() {
        return zkClusters;
    }

    public void setZkClusters(List<String> zkClusters) {
        this.zkClusters = zkClusters;
    }

    public MetaMode getMetaMode() {
        return metaMode;
    }

    public void setMetaMode(MetaMode metaMode) {
        this.metaMode = metaMode;
    }

    public StorageMode getStorageMode() {
        return storageMode;
    }

    public void setStorageMode(StorageMode storageMode) {
        this.storageMode = storageMode;
    }

    public Integer getMemoryStorageBufferSize() {
        return memoryStorageBufferSize;
    }

    public void setMemoryStorageBufferSize(Integer memoryStorageBufferSize) {
        this.memoryStorageBufferSize = memoryStorageBufferSize;
    }

    public String getFileStorageDirectory() {
        return fileStorageDirectory;
    }

    public void setFileStorageDirectory(String fileStorageDirectory) {
        this.fileStorageDirectory = fileStorageDirectory;
    }

    public Integer getFileStorageStoreCount() {
        return fileStorageStoreCount;
    }

    public void setFileStorageStoreCount(Integer fileStorageStoreCount) {
        this.fileStorageStoreCount = fileStorageStoreCount;
    }

    public Integer getFileStorageRollverCount() {
        return fileStorageRollverCount;
    }

    public void setFileStorageRollverCount(Integer fileStorageRollverCount) {
        this.fileStorageRollverCount = fileStorageRollverCount;
    }

    public Integer getFileStoragePercentThresold() {
        return fileStoragePercentThresold;
    }

    public void setFileStoragePercentThresold(Integer fileStoragePercentThresold) {
        this.fileStoragePercentThresold = fileStoragePercentThresold;
    }

    public SourcingType getSourcingType() {
        return sourcingType;
    }

    public void setSourcingType(SourcingType sourcingType) {
        this.sourcingType = sourcingType;
    }

    public String getLocalBinlogDirectory() {
        return localBinlogDirectory;
    }

    public void setLocalBinlogDirectory(String localBinlogDirectory) {
        this.localBinlogDirectory = localBinlogDirectory;
    }

    public HAMode getHaMode() {
        return haMode;
    }

    public void setHaMode(HAMode haMode) {
        this.haMode = haMode;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public Integer getDefaultConnectionTimeoutInSeconds() {
        return defaultConnectionTimeoutInSeconds;
    }

    public void setDefaultConnectionTimeoutInSeconds(Integer defaultConnectionTimeoutInSeconds) {
        this.defaultConnectionTimeoutInSeconds = defaultConnectionTimeoutInSeconds;
    }

    public Integer getReceiveBufferSize() {
        return receiveBufferSize;
    }

    public void setReceiveBufferSize(Integer receiveBufferSize) {
        this.receiveBufferSize = receiveBufferSize;
    }

    public Integer getSendBufferSize() {
        return sendBufferSize;
    }

    public void setSendBufferSize(Integer sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
    }

    public Byte getConnectionCharsetNumber() {
        return connectionCharsetNumber;
    }

    public void setConnectionCharsetNumber(Byte connectionCharsetNumber) {
        this.connectionCharsetNumber = connectionCharsetNumber;
    }

    public String getConnectionCharset() {
        return connectionCharset;
    }

    public void setConnectionCharset(String connectionCharset) {
        this.connectionCharset = connectionCharset;
    }

    public IndexMode getIndexMode() {
        return indexMode;
    }

    public void setIndexMode(IndexMode indexMode) {
        this.indexMode = indexMode;
    }

    public String getDefaultDatabaseName() {
        return defaultDatabaseName;
    }

    public void setDefaultDatabaseName(String defaultDatabaseName) {
        this.defaultDatabaseName = defaultDatabaseName;
    }

    public Long getSlaveId() {
        return slaveId;
    }

    public void setSlaveId(Long slaveId) {
        this.slaveId = slaveId;
    }

    public Boolean getDetectingEnable() {
        return detectingEnable;
    }

    public void setDetectingEnable(Boolean detectingEnable) {
        this.detectingEnable = detectingEnable;
    }

    public String getDetectingSQL() {
        return detectingSQL;
    }

    public void setDetectingSQL(String detectingSQL) {
        this.detectingSQL = detectingSQL;
    }

    public Integer getDetectingIntervalInSeconds() {
        return detectingIntervalInSeconds;
    }

    public void setDetectingIntervalInSeconds(Integer detectingIntervalInSeconds) {
        this.detectingIntervalInSeconds = detectingIntervalInSeconds;
    }

    public Integer getDetectingTimeoutThresholdInSeconds() {
        return detectingTimeoutThresholdInSeconds;
    }

    public void setDetectingTimeoutThresholdInSeconds(Integer detectingTimeoutThresholdInSeconds) {
        this.detectingTimeoutThresholdInSeconds = detectingTimeoutThresholdInSeconds;
    }

    public Integer getDetectingRetryTimes() {
        return detectingRetryTimes;
    }

    public void setDetectingRetryTimes(Integer detectingRetryTimes) {
        this.detectingRetryTimes = detectingRetryTimes;
    }

    public StorageScavengeMode getStorageScavengeMode() {
        return storageScavengeMode;
    }

    public void setStorageScavengeMode(StorageScavengeMode storageScavengeMode) {
        this.storageScavengeMode = storageScavengeMode;
    }

    public String getScavengeSchdule() {
        return scavengeSchdule;
    }

    public void setScavengeSchdule(String scavengeSchdule) {
        this.scavengeSchdule = scavengeSchdule;
    }

    public String getApp() {
        return app;
    }

    public String getGroup() {
        return group;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getMetaqStoreUri() {
        return metaqStoreUri;
    }

    public void setMetaqStoreUri(String metaqStoreUri) {
        this.metaqStoreUri = metaqStoreUri;
    }

    public Integer getTransactionSize() {
        return transactionSize != null ? transactionSize : 1024;
    }

    public void setTransactionSize(Integer transactionSize) {
        this.transactionSize = transactionSize;
    }

    public List<InetSocketAddress> getDbAddresses() {
        if (dbAddresses == null) {
            dbAddresses = new ArrayList<InetSocketAddress>();
            if (masterAddress != null) {
                dbAddresses.add(masterAddress);
            }

            if (standbyAddress != null) {
                dbAddresses.add(standbyAddress);
            }
        }
        return dbAddresses;
    }

    public List<List<DataSourcing>> getGroupDbAddresses() {
        if (groupDbAddresses == null) {
            groupDbAddresses = new ArrayList<List<DataSourcing>>();
            if (dbAddresses != null) {
                for (InetSocketAddress address : dbAddresses) {
                    List<DataSourcing> groupAddresses = new ArrayList<DataSourcing>();
                    groupAddresses.add(new DataSourcing(sourcingType, address));
                    groupDbAddresses.add(groupAddresses);
                }
            } else {
                if (masterAddress != null) {
                    List<DataSourcing> groupAddresses = new ArrayList<DataSourcing>();
                    groupAddresses.add(new DataSourcing(sourcingType, masterAddress));
                    groupDbAddresses.add(groupAddresses);
                }

                if (standbyAddress != null) {
                    List<DataSourcing> groupAddresses = new ArrayList<DataSourcing>();
                    groupAddresses.add(new DataSourcing(sourcingType, standbyAddress));
                    groupDbAddresses.add(groupAddresses);
                }
            }
        }
        return groupDbAddresses;
    }

    public void setGroupDbAddresses(List<List<DataSourcing>> groupDbAddresses) {
        this.groupDbAddresses = groupDbAddresses;
    }

    public void setDbAddresses(List<InetSocketAddress> dbAddresses) {
        this.dbAddresses = dbAddresses;
    }

    public String getDbUsername() {
        if (dbUsername == null) {
            dbUsername = (masterUsername != null ? masterUsername : standbyUsername);
        }
        return dbUsername;
    }

    public void setDbUsername(String dbUsername) {
        this.dbUsername = dbUsername;
    }

    public String getDbPassword() {
        if (dbPassword == null) {
            dbPassword = (masterPassword != null ? masterPassword : standbyPassword);
        }
        return dbPassword;
    }

    public void setDbPassword(String dbPassword) {
        this.dbPassword = dbPassword;
    }

    public List<String> getPositions() {
        if (positions == null) {
            positions = new ArrayList<String>();
            String masterPosition = buildPosition(masterLogfileName, masterLogfileOffest, masterTimestamp);
            if (masterPosition != null) {
                positions.add(masterPosition);
            }

            String standbyPosition = buildPosition(standbyLogfileName, standbyLogfileOffest, standbyTimestamp);
            if (standbyPosition != null) {
                positions.add(standbyPosition);
            }

        }
        return positions;
    }

    public void setPositions(List<String> positions) {
        this.positions = positions;
    }

    // ===========================兼容字段

    private String buildPosition(String journalName, Long position, Long timestamp) {
        StringBuilder masterBuilder = new StringBuilder();
        if (StringUtils.isNotEmpty(journalName) || position != null || timestamp != null) {
            masterBuilder.append('{');
            if (StringUtils.isNotEmpty(journalName)) {
                masterBuilder.append("\"journalName\":\"").append(journalName).append("\"");
            }

            if (position != null) {
                if (masterBuilder.length() > 1) {
                    masterBuilder.append(",");
                }
                masterBuilder.append("\"position\":").append(position);
            }

            if (timestamp != null) {
                if (masterBuilder.length() > 1) {
                    masterBuilder.append(",");
                }
                masterBuilder.append("\"timestamp\":").append(timestamp);
            }
            masterBuilder.append('}');
            return masterBuilder.toString();
        } else {
            return null;
        }
    }

    public void setMasterUsername(String masterUsername) {
        this.masterUsername = masterUsername;
    }

    public void setMasterPassword(String masterPassword) {
        this.masterPassword = masterPassword;
    }

    public void setStandbyAddress(InetSocketAddress standbyAddress) {
        this.standbyAddress = standbyAddress;
    }

    public void setStandbyUsername(String standbyUsername) {
        this.standbyUsername = standbyUsername;
    }

    public void setStandbyPassword(String standbyPassword) {
        this.standbyPassword = standbyPassword;
    }

    public void setMasterLogfileName(String masterLogfileName) {
        this.masterLogfileName = masterLogfileName;
    }

    public void setMasterLogfileOffest(Long masterLogfileOffest) {
        this.masterLogfileOffest = masterLogfileOffest;
    }

    public void setMasterTimestamp(Long masterTimestamp) {
        this.masterTimestamp = masterTimestamp;
    }

    public void setStandbyLogfileName(String standbyLogfileName) {
        this.standbyLogfileName = standbyLogfileName;
    }

    public void setStandbyLogfileOffest(Long standbyLogfileOffest) {
        this.standbyLogfileOffest = standbyLogfileOffest;
    }

    public void setStandbyTimestamp(Long standbyTimestamp) {
        this.standbyTimestamp = standbyTimestamp;
    }

    public void setMasterAddress(InetSocketAddress masterAddress) {
        this.masterAddress = masterAddress;
    }

    public Integer getFallbackIntervalInSeconds() {
        return fallbackIntervalInSeconds == null ? 60 : fallbackIntervalInSeconds;
    }

    public void setFallbackIntervalInSeconds(Integer fallbackIntervalInSeconds) {
        this.fallbackIntervalInSeconds = fallbackIntervalInSeconds;
    }

    public Boolean getHeartbeatHaEnable() {
        return heartbeatHaEnable == null ? false : true;
    }

    public void setHeartbeatHaEnable(Boolean heartbeatHaEnable) {
        this.heartbeatHaEnable = heartbeatHaEnable;
    }

    public BatchMode getStorageBatchMode() {
        return storageBatchMode == null ? BatchMode.MEMSIZE : storageBatchMode;
    }

    public void setStorageBatchMode(BatchMode storageBatchMode) {
        this.storageBatchMode = storageBatchMode;
    }

    public Integer getMemoryStorageBufferMemUnit() {
        return memoryStorageBufferMemUnit == null ? 1024 : memoryStorageBufferMemUnit;
    }

    public void setMemoryStorageBufferMemUnit(Integer memoryStorageBufferMemUnit) {
        this.memoryStorageBufferMemUnit = memoryStorageBufferMemUnit;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }
}
