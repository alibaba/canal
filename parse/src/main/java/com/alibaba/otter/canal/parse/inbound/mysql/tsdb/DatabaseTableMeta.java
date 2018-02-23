package com.alibaba.otter.canal.parse.inbound.mysql.tsdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.alibaba.druid.sql.repository.Schema;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
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

    private static Logger              logger        = LoggerFactory.getLogger(DatabaseTableMeta.class);
    private static Pattern             pattern       = Pattern.compile("Duplicate entry '.*' for key '*'");
    private static Pattern             h2Pattern     = Pattern.compile("Unique index or primary key violation");
    private static final EntryPosition INIT_POSITION = new EntryPosition("0", 0L, -2L, -1L);
    private String                     destination;
    private MemoryTableMeta            memoryTableMeta;
    private MysqlConnection            connection;                                                              // 查询meta信息的链接
    private CanalEventFilter           filter;
    private CanalEventFilter           blackFilter;
    private EntryPosition              lastPosition;
    private ScheduledExecutorService   scheduler;
    private MetaHistoryDAO             metaHistoryDAO;
    private MetaSnapshotDAO            metaSnapshotDAO;

    public DatabaseTableMeta(){

    }

    @Override
    public boolean init(final String destination) {
        this.destination = destination;
        this.memoryTableMeta = new MemoryTableMeta();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "[scheduler-table-meta-snapshot]");
            }
        });

        // 24小时生成一份snapshot
        scheduler.scheduleWithFixedDelay(new Runnable() {

            @Override
            public void run() {
                try {
                    MDC.put("destination", destination);
                    applySnapshotToDB(lastPosition, false);
                } catch (Throwable e) {
                    logger.error("scheudle applySnapshotToDB faield", e);
                }
            }
        }, 24, 24, TimeUnit.HOURS);
        return true;
    }

    @Override
    public TableMeta find(String schema, String table) {
        synchronized (memoryTableMeta) {
            return memoryTableMeta.find(schema, table);
        }
    }

    @Override
    public boolean apply(EntryPosition position, String schema, String ddl, String extra) {
        // 首先记录到内存结构
        synchronized (memoryTableMeta) {
            if (memoryTableMeta.apply(position, schema, ddl, extra)) {
                this.lastPosition = position;
                // 同步每次变更给远程做历史记录
                return applyHistoryToDB(position, schema, ddl, extra);
            } else {
                throw new RuntimeException("apply to memory is failed");
            }
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
                packet = connection.query("show tables from `" + schema + "`");
                List<String> tables = new ArrayList<String>();
                for (String table : packet.getFieldValues()) {
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
        MemoryTableMeta tmpMemoryTableMeta = new MemoryTableMeta();
        Map<String, String> schemaDdls = null;
        synchronized (memoryTableMeta) {
            if (!init && position == null) {
                // 如果是持续构建,则识别一下是否有DDL变更过,如果没有就忽略了
                return false;
            }
            schemaDdls = memoryTableMeta.snapshot();
            for (Map.Entry<String, String> entry : schemaDdls.entrySet()) {
                tmpMemoryTableMeta.apply(position, entry.getKey(), entry.getValue(), null);
            }
        }

        // 基于临时内存对象进行对比
        boolean compareAll = true;
        for (Schema schema : tmpMemoryTableMeta.getRepository().getSchemas()) {
            for (String table : schema.showTables()) {
                if (!compareTableMetaDbAndMemory(connection, tmpMemoryTableMeta, schema.getName(), table)) {
                    compareAll = false;
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
        try {
            ResultSetPacket packet = connection.query("desc " + getFullName(schema, table));
            tableMetaFromDB.setFields(TableMetaCache.parserTableMeta(packet));
        } catch (IOException e) {
            if (e.getMessage().contains("errorNumber=1146")) {
                logger.error("table not exist in db , pls check :" + getFullName(schema, table) + " , mem : "
                             + tableMetaFromMem);
                return false;
            }
            throw new CanalParseException(e);
        }

        boolean result = compareTableMeta(tableMetaFromMem, tableMetaFromDB);
        if (!result) {
            String createDDL = null;
            try {
                ResultSetPacket packet = connection.query("show create table " + getFullName(schema, table));
                if (packet.getFieldValues().size() > 1) {
                    createDDL = packet.getFieldValues().get(1);
                }
            } catch (IOException e) {
                // ignore
            }

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

    private boolean compareTableMeta(TableMeta source, TableMeta target) {
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

        for (int i = 0; i < sourceFields.size(); i++) {
            FieldMeta sourceField = sourceFields.get(i);
            FieldMeta targetField = targetFields.get(i);
            if (!StringUtils.equalsIgnoreCase(sourceField.getColumnName(), targetField.getColumnName())) {
                return false;
            }

            if (!StringUtils.equalsIgnoreCase(sourceField.getColumnType(), targetField.getColumnType())) {
                return false;
            }

            if (!StringUtils.equalsIgnoreCase(sourceField.getDefaultValue(), targetField.getDefaultValue())) {
                return false;
            }

            if (sourceField.isNullable() != targetField.isNullable()) {
                return false;
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
}
