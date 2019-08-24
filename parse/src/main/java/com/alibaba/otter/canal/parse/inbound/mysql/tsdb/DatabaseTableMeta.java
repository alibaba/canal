package com.alibaba.otter.canal.parse.inbound.mysql.tsdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastsql.sql.repository.Schema;
import com.alibaba.otter.canal.filter.CanalEventFilter;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ResultSetPacket;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.TableMeta;
import com.alibaba.otter.canal.parse.inbound.TableMeta.FieldMeta;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.TableMetaCache;
import com.alibaba.otter.canal.parse.inbound.mysql.ddl.DdlResult;
import com.alibaba.otter.canal.parse.inbound.mysql.ddl.DruidDdlParser;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.dao.MetaHistoryDAO;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.dao.MetaHistoryDO;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.dao.MetaSnapshotDAO;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.dao.MetaSnapshotDO;
import com.alibaba.otter.canal.protocol.position.EntryPosition;

/**
 * 基于db远程管理 see internal class: CanalTableMeta , ConsoleTableMetaTSDB
 *
 * @author agapple 2017年7月27日 下午10:47:55
 * @since 3.2.5
 */
public class DatabaseTableMeta implements TableMetaTSDB {

    public static final EntryPosition       INIT_POSITION       = new EntryPosition("0", 0L, -2L, -1L);
    private static Logger                   logger              = LoggerFactory.getLogger(DatabaseTableMeta.class);
    private static Pattern                  pattern             = Pattern.compile("Duplicate entry '.*' for key '*'");
    private static Pattern                  h2Pattern           = Pattern.compile("Unique index or primary key violation");
    private static ScheduledExecutorService scheduler           = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {

                                                                    @Override
                                                                    public Thread newThread(Runnable r) {
                                                                        Thread thread = new Thread(r,
                                                                            "[scheduler-table-meta-snapshot]");
                                                                        thread.setDaemon(true);
                                                                        return thread;
                                                                    }
                                                                });
    private ReadWriteLock                   lock                = new ReentrantReadWriteLock();
    private AtomicBoolean                   initialized         = new AtomicBoolean(false);
    private String                          destination;
    private MemoryTableMeta                 memoryTableMeta;
    private volatile MysqlConnection        connection;                                                                    // 查询meta信息的链接
    private CanalEventFilter                filter;
    private CanalEventFilter                blackFilter;
    private Map<String, List<String>>       fieldFilterMap      = new HashMap<String, List<String>>();
    private Map<String, List<String>>       fieldBlackFilterMap = new HashMap<String, List<String>>();
    private EntryPosition                   lastPosition;
    private boolean                         hasNewDdl;
    private MetaHistoryDAO                  metaHistoryDAO;
    private MetaSnapshotDAO                 metaSnapshotDAO;
    private int                             snapshotInterval    = 24;
    private int                             snapshotExpire      = 360;
    private ScheduledFuture<?>              scheduleSnapshotFuture;

    public DatabaseTableMeta(){

    }

    @Override
    public boolean init(final String destination) {
        if (initialized.compareAndSet(false, true)) {
            this.destination = destination;
            this.memoryTableMeta = new MemoryTableMeta();

            // 24小时生成一份snapshot
            if (snapshotInterval > 0) {
                scheduleSnapshotFuture = scheduler.scheduleWithFixedDelay(new Runnable() {

                    @Override
                    public void run() {
                        boolean applyResult = false;
                        try {
                            MDC.put("destination", destination);
                            applyResult = applySnapshotToDB(lastPosition, false);
                        } catch (Throwable e) {
                            logger.error("scheudle applySnapshotToDB faield", e);
                        }

                        try {
                            MDC.put("destination", destination);
                            if (applyResult) {
                                snapshotExpire((int) TimeUnit.HOURS.toSeconds(snapshotExpire));
                            }
                        } catch (Throwable e) {
                            logger.error("scheudle snapshotExpire faield", e);
                        }
                    }
                }, snapshotInterval, snapshotInterval, TimeUnit.HOURS);
            }
        }
        return true;
    }

    @Override
    public void destory() {
        if (memoryTableMeta != null) {
            memoryTableMeta.destory();
        }

        if (connection != null) {
            try {
                connection.disconnect();
            } catch (IOException e) {
                logger.error("ERROR # disconnect meta connection for address:{}", connection.getConnector()
                    .getAddress(), e);
            }
        }

        if (scheduleSnapshotFuture != null) {
            scheduleSnapshotFuture.cancel(false);
        }
    }

    @Override
    public TableMeta find(String schema, String table) {
        lock.readLock().lock();
        try {
            return memoryTableMeta.find(schema, table);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean apply(EntryPosition position, String schema, String ddl, String extra) {
        // 首先记录到内存结构
        lock.writeLock().lock();
        try {
            if (memoryTableMeta.apply(position, schema, ddl, extra)) {
                this.lastPosition = position;
                this.hasNewDdl = true;
                // 同步每次变更给远程做历史记录
                return applyHistoryToDB(position, schema, ddl, extra);
            } else {
                throw new RuntimeException("apply to memory is failed");
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public boolean rollback(EntryPosition position) {
        // 每次rollback需要重新构建一次memory data
        this.memoryTableMeta = new MemoryTableMeta();
        boolean flag = false;
        EntryPosition snapshotPosition = buildMemFromSnapshot(position);
        if (snapshotPosition != null) {
            applyHistoryOnMemory(snapshotPosition, position);
            flag = true;
        }

        if (!flag) {
            // 如果没有任何数据，则为初始化状态，全量dump一份关注的表
            if (dumpTableMeta(connection, filter)) {
                // 记录一下snapshot结果,方便快速恢复
                flag = applySnapshotToDB(INIT_POSITION, true);
            }
        }

        return flag;
    }

    @Override
    public Map<String, String> snapshot() {
        return memoryTableMeta.snapshot();
    }

    /**
     * 初始化的时候dump一下表结构
     */
    private boolean dumpTableMeta(MysqlConnection connection, final CanalEventFilter filter) {
        try {
            ResultSetPacket packet = connection.query("show databases");
            List<String> schemas = new ArrayList<String>();
            for (String schema : packet.getFieldValues()) {
                schemas.add(schema);
            }

            for (String schema : schemas) {
                // filter views
                packet = connection.query("show full tables from `" + schema + "` where Table_type = 'BASE TABLE'");
                List<String> tables = new ArrayList<String>();
                for (String table : packet.getFieldValues()) {
                    if ("BASE TABLE".equalsIgnoreCase(table)) {
                        continue;
                    }
                    String fullName = schema + "." + table;
                    if (blackFilter == null || !blackFilter.filter(fullName)) {
                        if (filter == null || filter.filter(fullName)) {
                            tables.add(table);
                        }
                    }
                }

                if (tables.isEmpty()) {
                    continue;
                }

                StringBuilder sql = new StringBuilder();
                for (String table : tables) {
                    sql.append("show create table `" + schema + "`.`" + table + "`;");
                }

                List<ResultSetPacket> packets = connection.queryMulti(sql.toString());
                for (ResultSetPacket onePacket : packets) {
                    if (onePacket.getFieldValues().size() > 1) {
                        String oneTableCreateSql = onePacket.getFieldValues().get(1);
                        memoryTableMeta.apply(INIT_POSITION, schema, oneTableCreateSql, null);
                    }
                }
            }

            return true;
        } catch (IOException e) {
            throw new CanalParseException(e);
        }
    }

    private boolean applyHistoryToDB(EntryPosition position, String schema, String ddl, String extra) {
        Map<String, String> content = new HashMap<String, String>();
        content.put("destination", destination);
        content.put("binlogFile", position.getJournalName());
        content.put("binlogOffest", String.valueOf(position.getPosition()));
        content.put("binlogMasterId", String.valueOf(position.getServerId()));
        content.put("binlogTimestamp", String.valueOf(position.getTimestamp()));
        content.put("useSchema", schema);
        if (content.isEmpty()) {
            throw new RuntimeException("apply failed caused by content is empty in applyHistoryToDB");
        }
        // 待补充
        List<DdlResult> ddlResults = DruidDdlParser.parse(ddl, schema);
        if (ddlResults.size() > 0) {
            DdlResult ddlResult = ddlResults.get(0);
            content.put("sqlSchema", ddlResult.getSchemaName());
            content.put("sqlTable", ddlResult.getTableName());
            content.put("sqlType", ddlResult.getType().name());
            content.put("sqlText", ddl);
            content.put("extra", extra);
        }

        MetaHistoryDO metaDO = new MetaHistoryDO();
        try {
            BeanUtils.populate(metaDO, content);
            // 会建立唯一约束,解决:
            // 1. 重复的binlog file+offest
            // 2. 重复的masterId+timestamp
            metaHistoryDAO.insert(metaDO);
        } catch (Throwable e) {
            if (isUkDuplicateException(e)) {
                // 忽略掉重复的位点
                logger.warn("dup apply for sql : " + ddl);
            } else {
                throw new CanalParseException("apply history to db failed caused by : " + e.getMessage(), e);
            }

        }
        return true;
    }

    /**
     * 发布数据到console上
     */
    private boolean applySnapshotToDB(EntryPosition position, boolean init) {
        // 获取一份快照
        Map<String, String> schemaDdls = null;
        lock.readLock().lock();
        try {
            if (!init && !hasNewDdl) {
                // 如果是持续构建,则识别一下是否有DDL变更过,如果没有就忽略了
                return false;
            }
            this.hasNewDdl = false;
            schemaDdls = memoryTableMeta.snapshot();
        } finally {
            lock.readLock().unlock();
        }

        MemoryTableMeta tmpMemoryTableMeta = new MemoryTableMeta();
        for (Map.Entry<String, String> entry : schemaDdls.entrySet()) {
            tmpMemoryTableMeta.apply(position, entry.getKey(), entry.getValue(), null);
        }

        // 基于临时内存对象进行对比
        boolean compareAll = true;
        for (Schema schema : tmpMemoryTableMeta.getRepository().getSchemas()) {
            for (String table : schema.showTables()) {
                String fullName = schema + "." + table;
                if (blackFilter == null || !blackFilter.filter(fullName)) {
                    if (filter == null || filter.filter(fullName)) {
                        // issue : https://github.com/alibaba/canal/issues/1168
                        // 在生成snapshot时重新过滤一遍
                        if (!compareTableMetaDbAndMemory(connection, tmpMemoryTableMeta, schema.getName(), table)) {
                            compareAll = false;
                        }
                    }
                }
            }
        }

        if (compareAll) {
            Map<String, String> content = new HashMap<String, String>();
            content.put("destination", destination);
            content.put("binlogFile", position.getJournalName());
            content.put("binlogOffest", String.valueOf(position.getPosition()));
            content.put("binlogMasterId", String.valueOf(position.getServerId()));
            content.put("binlogTimestamp", String.valueOf(position.getTimestamp()));
            content.put("data", JSON.toJSONString(schemaDdls));
            if (content.isEmpty()) {
                throw new RuntimeException("apply failed caused by content is empty in applySnapshotToDB");
            }

            MetaSnapshotDO snapshotDO = new MetaSnapshotDO();
            try {
                BeanUtils.populate(snapshotDO, content);
                metaSnapshotDAO.insert(snapshotDO);
            } catch (Throwable e) {
                if (isUkDuplicateException(e)) {
                    // 忽略掉重复的位点
                    logger.info("dup apply snapshot use position : " + position + " , just ignore");
                } else {
                    throw new CanalParseException("apply failed caused by : " + e.getMessage(), e);
                }
            }
            return true;
        } else {
            logger.error("compare failed , check log");
        }
        return false;
    }

    private boolean compareTableMetaDbAndMemory(MysqlConnection connection, MemoryTableMeta memoryTableMeta,
                                                final String schema, final String table) {
        TableMeta tableMetaFromMem = memoryTableMeta.find(schema, table);

        TableMeta tableMetaFromDB = new TableMeta();
        tableMetaFromDB.setSchema(schema);
        tableMetaFromDB.setTable(table);
        String createDDL = null;
        try {
            ResultSetPacket packet = connection.query("show create table " + getFullName(schema, table));
            if (packet.getFieldValues().size() > 1) {
                createDDL = packet.getFieldValues().get(1);
                tableMetaFromDB.setFields(TableMetaCache.parseTableMeta(schema, table, packet));
            }
        } catch (Throwable e) {
            try {
                // retry for broke pipe, see:
                // https://github.com/alibaba/canal/issues/724
                connection.reconnect();
                ResultSetPacket packet = connection.query("show create table " + getFullName(schema, table));
                if (packet.getFieldValues().size() > 1) {
                    createDDL = packet.getFieldValues().get(1);
                    tableMetaFromDB.setFields(TableMetaCache.parseTableMeta(schema, table, packet));
                }
            } catch (IOException e1) {
                if (e.getMessage().contains("errorNumber=1146")) {
                    logger.error("table not exist in db , pls check :" + getFullName(schema, table) + " , mem : "
                                 + tableMetaFromMem);
                    return false;
                }
                throw new CanalParseException(e);
            }
        }

        boolean result = compareTableMeta(tableMetaFromMem, tableMetaFromDB);
        if (!result) {
            logger.error("pls submit github issue, show create table ddl:" + createDDL + " , compare failed . \n db : "
                         + tableMetaFromDB + " \n mem : " + tableMetaFromMem);
        }
        return result;
    }

    private EntryPosition buildMemFromSnapshot(EntryPosition position) {
        try {
            MetaSnapshotDO snapshotDO = metaSnapshotDAO.findByTimestamp(destination, position.getTimestamp());
            if (snapshotDO == null) {
                return null;
            }
            String binlogFile = snapshotDO.getBinlogFile();
            Long binlogOffest = snapshotDO.getBinlogOffest();
            String binlogMasterId = snapshotDO.getBinlogMasterId();
            Long binlogTimestamp = snapshotDO.getBinlogTimestamp();

            EntryPosition snapshotPosition = new EntryPosition(binlogFile,
                binlogOffest == null ? 0l : binlogOffest,
                binlogTimestamp == null ? 0l : binlogTimestamp,
                Long.valueOf(binlogMasterId == null ? "-2" : binlogMasterId));
            // data存储为Map<String,String>，每个分库一套建表
            String sqlData = snapshotDO.getData();
            JSONObject jsonObj = JSON.parseObject(sqlData);
            for (Map.Entry entry : jsonObj.entrySet()) {
                // 记录到内存
                if (!memoryTableMeta.apply(snapshotPosition,
                    ObjectUtils.toString(entry.getKey()),
                    ObjectUtils.toString(entry.getValue()),
                    null)) {
                    return null;
                }
            }

            return snapshotPosition;
        } catch (Throwable e) {
            throw new CanalParseException("apply failed caused by : " + e.getMessage(), e);
        }
    }

    private boolean applyHistoryOnMemory(EntryPosition position, EntryPosition rollbackPosition) {
        try {
            List<MetaHistoryDO> metaHistoryDOList = metaHistoryDAO.findByTimestamp(destination,
                position.getTimestamp(),
                rollbackPosition.getTimestamp());
            if (metaHistoryDOList == null) {
                return true;
            }

            for (MetaHistoryDO metaHistoryDO : metaHistoryDOList) {
                String binlogFile = metaHistoryDO.getBinlogFile();
                Long binlogOffest = metaHistoryDO.getBinlogOffest();
                String binlogMasterId = metaHistoryDO.getBinlogMasterId();
                Long binlogTimestamp = metaHistoryDO.getBinlogTimestamp();
                String useSchema = metaHistoryDO.getUseSchema();
                String sqlData = metaHistoryDO.getSqlText();
                EntryPosition snapshotPosition = new EntryPosition(binlogFile,
                    binlogOffest == null ? 0L : binlogOffest,
                    binlogTimestamp == null ? 0L : binlogTimestamp,
                    Long.valueOf(binlogMasterId == null ? "-2" : binlogMasterId));

                // 如果是同一秒内,对比一下history的位点，如果比期望的位点要大，忽略之
                if (snapshotPosition.getTimestamp() > rollbackPosition.getTimestamp()) {
                    continue;
                } else if (rollbackPosition.getServerId() == snapshotPosition.getServerId()
                           && snapshotPosition.compareTo(rollbackPosition) > 0) {
                    continue;
                }

                // 记录到内存
                if (!memoryTableMeta.apply(snapshotPosition, useSchema, sqlData, null)) {
                    return false;
                }

            }

            return metaHistoryDOList.size() > 0;
        } catch (Throwable e) {
            throw new CanalParseException("apply failed", e);
        }
    }

    private String getFullName(String schema, String table) {
        StringBuilder builder = new StringBuilder();
        return builder.append('`')
            .append(schema)
            .append('`')
            .append('.')
            .append('`')
            .append(table)
            .append('`')
            .toString();
    }

    public static boolean compareTableMeta(TableMeta source, TableMeta target) {
        if (!StringUtils.equalsIgnoreCase(source.getSchema(), target.getSchema())) {
            return false;
        }

        if (!StringUtils.equalsIgnoreCase(source.getTable(), target.getTable())) {
            return false;
        }

        List<FieldMeta> sourceFields = source.getFields();
        List<FieldMeta> targetFields = target.getFields();
        if (sourceFields.size() != targetFields.size()) {
            return false;
        }

        /**
         * MySQL DDL的一些默认行为:
         * 
         * <pre>
         * 1. Timestamp类型的列在第一次添加时，未指定默认值会默认为CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
         * 2. Timestamp类型的列在第二次时，必须指定默认值
         * 3. BLOB和TEXT类型不存在NULL、NOT NULL属性
         * 4. 部分数据类型是synonyms，实际show create table时会转成对应类型
         * 5. 非BLOB和TEXT类型在默认未指定NULL、NOT NULL时，默认default null
         * 6. 在列变更时，不仅变更列名数据类型，同时各个约束中列名也会变更，同时如果约束中包含key length，则变更后的数据类型不应违背key length的约束（有长度的应大于key length；BLOB、TEXT应有key length；可以在存在key length情况下变更为无key length的数据类型，约束中key length忽略；等等）
         * 7. 字符集每列（char类、eumn、set）默认保存，指定使用指定的，未指定使用表默认的，不受修改表默认字符集而改变，同表默认时，字符集显示省略
         * 8. 新建表默认innodb引擎，latin1字符集
         * 9. BLOB、TEXT会根据给定长度自动转换为对应的TINY、MEDIUM，LONG类型，长度和字符集也有关
         * 10. unique约束在没有指定索引名是非幂等的，会自动以约束索引第一个列名称命名，同时以_2,_3这种形式添加后缀
         * </pre>
         */

        for (int i = 0; i < sourceFields.size(); i++) {
            FieldMeta sourceField = sourceFields.get(i);
            FieldMeta targetField = targetFields.get(i);
            if (!StringUtils.equalsIgnoreCase(sourceField.getColumnName(), targetField.getColumnName())) {
                return false;
            }

            // if (!StringUtils.equalsIgnoreCase(sourceField.getColumnType(),
            // targetField.getColumnType())) {
            // return false;
            // }

            // https://github.com/alibaba/canal/issues/1100
            // 支持一下 int vs int(10)
            if ((sourceField.isUnsigned() && !targetField.isUnsigned())
                || (!sourceField.isUnsigned() && targetField.isUnsigned())) {
                return false;
            }

            String sourceColumnType = StringUtils.removeEndIgnoreCase(sourceField.getColumnType(), "zerofill").trim();
            String targetColumnType = StringUtils.removeEndIgnoreCase(targetField.getColumnType(), "zerofill").trim();

            String sign = sourceField.isUnsigned() ? "unsigned" : "signed";
            sourceColumnType = StringUtils.removeEndIgnoreCase(sourceColumnType, sign).trim();
            targetColumnType = StringUtils.removeEndIgnoreCase(targetColumnType, sign).trim();

            boolean columnTypeCompare = false;
            columnTypeCompare |= StringUtils.containsIgnoreCase(sourceColumnType, targetColumnType);
            columnTypeCompare |= StringUtils.containsIgnoreCase(targetColumnType, sourceColumnType);
            if (!columnTypeCompare) {
                // 去掉精度参数再对比一次
                sourceColumnType = synonymsType(StringUtils.substringBefore(sourceColumnType, "(")).trim();
                targetColumnType = synonymsType(StringUtils.substringBefore(targetColumnType, "(")).trim();
                columnTypeCompare |= StringUtils.containsIgnoreCase(sourceColumnType, targetColumnType);
                columnTypeCompare |= StringUtils.containsIgnoreCase(targetColumnType, sourceColumnType);
                if (!columnTypeCompare) {
                    return false;
                }
            }

            // if (!StringUtils.equalsIgnoreCase(sourceField.getDefaultValue(),
            // targetField.getDefaultValue())) {
            // return false;
            // }

            // BLOB, TEXT, GEOMETRY or JSON默认都是nullable，可以忽略比较，但比较了也是对齐
            if (StringUtils.containsIgnoreCase(sourceColumnType, "timestamp")
                || StringUtils.containsIgnoreCase(targetColumnType, "timestamp")) {
                // timestamp可能会加default current_timestamp默认值,忽略对比nullable
            } else {
                if (sourceField.isNullable() != targetField.isNullable()) {
                    return false;
                }
            }

            // mysql会有一种处理,针对show create只有uk没有pk时，会在desc默认将uk当做pk
            boolean isSourcePkOrUk = sourceField.isKey() || sourceField.isUnique();
            boolean isTargetPkOrUk = targetField.isKey() || targetField.isUnique();
            if (isSourcePkOrUk != isTargetPkOrUk) {
                return false;
            }
        }

        return true;
    }

    /**
     * <pre>
     * synonyms处理 
     * 1. BOOL/BOOLEAN => TINYINT 
     * 2. DEC/NUMERIC/FIXED => DECIMAL
     * 3. INTEGER => INT
     * 
     * 
     * </pre>
     * 
     * @param originType
     * @return
     */
    private static String synonymsType(String originType) {
        if (StringUtils.equalsIgnoreCase(originType, "bool") || StringUtils.equalsIgnoreCase(originType, "boolean")) {
            return "tinyint";
        } else if (StringUtils.equalsIgnoreCase(originType, "dec")
                   || StringUtils.equalsIgnoreCase(originType, "numeric")
                   || StringUtils.equalsIgnoreCase(originType, "fixed")) {
            return "decimal";
        } else if (StringUtils.equalsIgnoreCase(originType, "integer")) {
            return "int";
        } else if (StringUtils.equalsIgnoreCase(originType, "real")
                   || StringUtils.equalsIgnoreCase(originType, "double precision")) {
            return "double";
        }

        // BLOB、TEXT会根据给定长度自动转换为对应的TINY、MEDIUM，LONG类型，长度和字符集也有关，统一按照blob对比
        if (StringUtils.equalsIgnoreCase(originType, "tinyblob")
            || StringUtils.equalsIgnoreCase(originType, "mediumblob")
            || StringUtils.equalsIgnoreCase(originType, "longblob")) {
            return "blob";
        } else if (StringUtils.equalsIgnoreCase(originType, "tinytext")
                   || StringUtils.equalsIgnoreCase(originType, "mediumtext")
                   || StringUtils.equalsIgnoreCase(originType, "longtext")) {
            return "text";
        }

        return originType;
    }

    private int snapshotExpire(int expireTimestamp) {
        return metaSnapshotDAO.deleteByTimestamp(destination, expireTimestamp);
    }

    public void setConnection(MysqlConnection connection) {
        this.connection = connection;
    }

    public void setFilter(CanalEventFilter filter) {
        this.filter = filter;
    }

    public MetaHistoryDAO getMetaHistoryDAO() {
        return metaHistoryDAO;
    }

    public void setMetaHistoryDAO(MetaHistoryDAO metaHistoryDAO) {
        this.metaHistoryDAO = metaHistoryDAO;
    }

    public MetaSnapshotDAO getMetaSnapshotDAO() {
        return metaSnapshotDAO;
    }

    public void setMetaSnapshotDAO(MetaSnapshotDAO metaSnapshotDAO) {
        this.metaSnapshotDAO = metaSnapshotDAO;
    }

    public void setBlackFilter(CanalEventFilter blackFilter) {
        this.blackFilter = blackFilter;
    }

    public void setFieldFilterMap(Map<String, List<String>> fieldFilterMap) {
        this.fieldFilterMap = fieldFilterMap;
    }

    public void setFieldBlackFilterMap(Map<String, List<String>> fieldBlackFilterMap) {
        this.fieldBlackFilterMap = fieldBlackFilterMap;
    }

    public int getSnapshotInterval() {
        return snapshotInterval;
    }

    public void setSnapshotInterval(int snapshotInterval) {
        this.snapshotInterval = snapshotInterval;
    }

    public int getSnapshotExpire() {
        return snapshotExpire;
    }

    public void setSnapshotExpire(int snapshotExpire) {
        this.snapshotExpire = snapshotExpire;
    }

    public MysqlConnection getConnection() {
        return connection;
    }

    public boolean isUkDuplicateException(Throwable t) {
        if (pattern.matcher(t.getMessage()).find() || h2Pattern.matcher(t.getMessage()).find()) {
            // 违反外键约束时也抛出这种异常，所以这里还要判断包含字符串Duplicate entry
            return true;
        }
        return false;
    }

    public static void main(String[] args) {
        String str = StringUtils.substringBefore("int(11)", "(");
        System.out.println(str);
    }
}
