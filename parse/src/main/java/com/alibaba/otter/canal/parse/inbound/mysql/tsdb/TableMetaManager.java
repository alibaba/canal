package com.alibaba.otter.canal.parse.inbound.mysql.tsdb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import javax.annotation.Resource;

import com.alibaba.druid.sql.repository.Schema;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.filter.CanalEventFilter;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.TableMeta;
import com.alibaba.otter.canal.parse.inbound.TableMeta.FieldMeta;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection.ProcessJdbcResult;
import com.alibaba.otter.canal.parse.inbound.mysql.ddl.DdlResult;
import com.alibaba.otter.canal.parse.inbound.mysql.ddl.DruidDdlParser;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.dao.MetaHistoryDAO;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.dao.MetaSnapshotDAO;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.model.MetaHistoryDO;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.model.MetaSnapshotDO;

import com.taobao.tddl.dbsync.binlog.BinlogPosition;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于console远程管理
 *
 * see internal class: CanalTableMeta , ConsoleTableMetaTSDB
 *
 * @author agapple 2017年7月27日 下午10:47:55
 * @since 3.2.5
 */
public class TableMetaManager implements TableMetaTSDB {
    private static Pattern pattern = Pattern.compile("Duplicate entry '.*' for key '*'");

    private static final BinlogPosition INIT_POSITION = BinlogPosition.parseFromString("0:0#-2.-1");
    private Logger logger = LoggerFactory.getLogger(TableMetaManager.class);
    private String consoleDomain = null;
    private int retry = 3;
    private MemoryTableMeta memoryTableMeta;
    private MysqlConnection connection; // 查询meta信息的链接
    private CanalEventFilter filter;
    private BinlogPosition lastPosition;
    private ScheduledExecutorService scheduler;

    @Resource
    private MetaHistoryDAO metaHistoryDAO;

    @Resource
    private MetaSnapshotDAO metaSnapshotDAO;

    public void init(){
        this.memoryTableMeta = new MemoryTableMeta(logger);
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
                    logger.info("-------- begin to produce snapshot for table meta");
                    applySnapshotToDB(lastPosition, false);
                } catch (Throwable e) {
                    logger.error("scheudle faield", e);
                }
            }
        }, 24, 24, TimeUnit.SECONDS);
    }

    public TableMetaManager() {

    }

    @Override
    public TableMeta find(String schema, String table) {
        synchronized (memoryTableMeta) {
            return memoryTableMeta.find(schema, table);
        }
    }

    @Override
    public boolean apply(BinlogPosition position, String schema, String ddl) {
        // 首先记录到内存结构
        synchronized (memoryTableMeta) {
            if (memoryTableMeta.apply(position, schema, ddl)) {
                this.lastPosition = position;
                // 同步每次变更给远程做历史记录
                return applyHistoryToDB(position, schema, ddl);
            } else {
                throw new RuntimeException("apply to memory is failed");
            }
        }
    }

    @Override
    public boolean rollback(BinlogPosition position) {
        // 每次rollback需要重新构建一次memory data
        this.memoryTableMeta = new MemoryTableMeta(logger);
        boolean flag = false;
        BinlogPosition snapshotPosition = buildMemFromSnapshot(position);
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
        List<String> schemas = connection.query("show databases", new ProcessJdbcResult<List>() {

            @Override
            public List process(ResultSet rs) throws SQLException {
                List<String> schemas = new ArrayList<String>();
                while (rs.next()) {
                    String schema = rs.getString(1);
                    if (!filter.filter(schema)) {
                        schemas.add(schema);
                    }
                }
                return schemas;
            }
        });

        for (String schema : schemas) {
            List<String> tables = connection.query("show tables from `" + schema + "`", new ProcessJdbcResult<List>() {

                @Override
                public List process(ResultSet rs) throws SQLException {
                    List<String> tables = new ArrayList<String>();
                    while (rs.next()) {
                        String table = rs.getString(1);
                        if (!filter.filter(table)) {
                            tables.add(table);
                        }
                    }
                    return tables;
                }
            });

            StringBuilder sql = new StringBuilder();
            for (String table : tables) {
                sql.append("show create table `" + schema + "`.`" + table + "`;");
            }

            // 使用多语句方式读取
            Statement stmt = null;
            try {
                stmt = connection.getConn().createStatement();
                ResultSet rs = stmt.executeQuery(sql.toString());
                boolean existMoreResult = false;
                do {
                    if (existMoreResult) {
                        rs = stmt.getResultSet();
                    }

                    while (rs.next()) {
                        String oneTableCreateSql = rs.getString(2);
                        memoryTableMeta.apply(INIT_POSITION, schema, oneTableCreateSql);
                    }

                    existMoreResult = stmt.getMoreResults();
                } while (existMoreResult);
            } catch (SQLException e) {
                throw new CanalParseException(e);
            } finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException e) {
                        // ignore
                    }
                }
            }
        }

        return true;
    }

    private boolean applyHistoryToDB(BinlogPosition position, String schema, String ddl) {

        Map<String, String> content = new HashMap<String, String>();

        content.put("binlogFile", position.getFileName());
        content.put("binlogOffest", String.valueOf(position.getPosition()));
        content.put("binlogMasterId", String.valueOf(position.getMasterId()));
        content.put("binlogTimestamp", String.valueOf(position.getTimestamp()));
        content.put("useSchema", schema);
        if (content.isEmpty()) {
            throw new RuntimeException("apply failed caused by content is empty in applyHistoryToDB");
        }
        // 待补充
        List<DdlResult> ddlResults = DruidDdlParser.parse(schema, ddl);
        if (ddlResults.size() > 0) {
            DdlResult ddlResult = ddlResults.get(0);
            content.put("schema", ddlResult.getSchemaName());
            content.put("table", ddlResult.getTableName());
            content.put("type", ddlResult.getType().name());
            content.put("sql", ddl);
            // content.put("extra", "");
        }

        for (int i = 0; i < retry; i++) {
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
                    throw new RuntimeException("apply history to db failed caused by : " + e.getMessage());
                }

            }
            return true;
        }
        return false;
    }

    /**
     * 发布数据到console上
     */
    private boolean applySnapshotToDB(BinlogPosition position, boolean init) {
        // 获取一份快照
        MemoryTableMeta tmpMemoryTableMeta = new MemoryTableMeta(logger);
        Map<String, String> schemaDdls = null;
        synchronized (memoryTableMeta) {
            if (!init && position == null) {
                // 如果是持续构建,则识别一下是否有DDL变更过,如果没有就忽略了
                return false;
            }
            schemaDdls = memoryTableMeta.snapshot();
            for (Map.Entry<String, String> entry : schemaDdls.entrySet()) {
                tmpMemoryTableMeta.apply(position, entry.getKey(), entry.getValue());
            }
        }

        // 基于临时内存对象进行对比
        boolean compareAll = true;
        for (Schema schema : tmpMemoryTableMeta.getRepository().getSchemas()) {
            for (String table : schema.showTables()) {
                if (!compareTableMetaDbAndMemory(connection, schema.getName(), table)) {
                    compareAll = false;
                }
            }
        }
        if (compareAll) {

            Map<String, String> content = new HashMap<String, String>();

            content.put("binlogFile", position.getFileName());
            content.put("binlogOffest", String.valueOf(position.getPosition()));
            content.put("binlogMasterId", String.valueOf(position.getMasterId()));
            content.put("binlogTimestamp", String.valueOf(position.getTimestamp()));
            content.put("data", JSON.toJSONString(schemaDdls));
            if (content.isEmpty()) {
                throw new RuntimeException("apply failed caused by content is empty in applySnapshotToDB");
            }

            for (int i = 0; i < retry; i++) {
                MetaSnapshotDO snapshotDO = new MetaSnapshotDO();
                try {
                    BeanUtils.populate(snapshotDO, content);
                    metaSnapshotDAO.insert(snapshotDO);
                } catch (Throwable e) {
                    if (isUkDuplicateException(e)) {
                        // 忽略掉重复的位点
                        logger.warn("dup apply snapshot for data : " + snapshotDO.getData());
                    } else {
                        throw new RuntimeException("apply failed caused by : " + e.getMessage());
                    }
                }
                return true;

            }
            return false;

        } else {
            logger.error("compare failed , check log");
        }
        return false;
    }

    private boolean compareTableMetaDbAndMemory(MysqlConnection connection, final String schema, final String table) {
        TableMeta tableMetaFromDB = connection.query("desc " + getFullName(schema, table),
            new ProcessJdbcResult<TableMeta>() {

                @Override
                public TableMeta process(ResultSet rs) throws SQLException {
                    List<FieldMeta> metas = new ArrayList<FieldMeta>();
                    while (rs.next()) {
                        FieldMeta meta = new FieldMeta();
                        // 做一个优化，使用String.intern()，共享String对象，减少内存使用
                        meta.setColumnName(rs.getString("Field"));
                        meta.setColumnType(rs.getString("Type"));
                        meta.setNullable(StringUtils.equalsIgnoreCase(rs.getString("Null"), "YES"));
                        meta.setKey("PRI".equalsIgnoreCase(rs.getString("Key")));
                        meta.setDefaultValue(rs.getString("Default"));
                        metas.add(meta);
                    }

                    return new TableMeta(schema, table, metas);
                }
            });

        TableMeta tableMetaFromMem = memoryTableMeta.find(schema, table);
        boolean result = compareTableMeta(tableMetaFromMem, tableMetaFromDB);
        if (!result) {
            logger.error("compare failed . \n db : " + tableMetaFromDB + " \n mem : " + tableMetaFromMem);
        }

        return result;
    }

    private BinlogPosition buildMemFromSnapshot(BinlogPosition position) {

        Map<String, String> content = new HashMap<String, String>();

        content.put("binlogFile", position.getFileName());
        content.put("binlogOffest", String.valueOf(position.getPosition()));
        content.put("binlogMasterId", String.valueOf(position.getMasterId()));
        content.put("binlogTimestamp", String.valueOf(position.getTimestamp()));
        if (content.isEmpty()) {
            throw new RuntimeException("apply failed caused by content is empty in buildMemFromSnapshot");
        }
        for (int i = 0; i < retry; i++) {

            try {

                String timestamp = content.get("binlogTimestamp");
                MetaSnapshotDO snapshotDO = metaSnapshotDAO.findByTimestamp(Long.valueOf(timestamp));
                JSONObject jsonData = new JSONObject();
                jsonData.put("content", JSON.toJSONString(snapshotDO));
                if (jsonData == null) {
                    // 可能没有任何snapshot数据
                    return null;
                }

                String binlogFile = jsonData.getString("binlogFile");
                String binlogOffest = jsonData.getString("binlogOffest");
                String binlogMasterId = jsonData.getString("binlogMasterId");
                String binlogTimestamp = jsonData.getString("binlogTimestamp");

                BinlogPosition snapshotPosition = new BinlogPosition(binlogFile,
                    Long.valueOf(binlogOffest == null ? "0" : binlogOffest),
                    Long.valueOf(binlogMasterId == null ? "-2" : binlogMasterId),
                    Long.valueOf(binlogTimestamp == null ? "0" : binlogTimestamp));
                // data存储为Map<String,String>，每个分库一套建表
                String sqlData = jsonData.getString("data");
                JSONObject jsonObj = JSON.parseObject(sqlData);
                for (Map.Entry entry : jsonObj.entrySet()) {
                    // 记录到内存
                    if (!memoryTableMeta.apply(snapshotPosition,
                        ObjectUtils.toString(entry.getKey()),
                        ObjectUtils.toString(entry.getValue()))) {
                        return null;
                    }
                }

                return snapshotPosition;

            } catch (Throwable e) {
                throw new RuntimeException("apply failed caused by : " + e.getMessage());
            }

        }

        return null;

    }

    private boolean applyHistoryOnMemory(BinlogPosition position, BinlogPosition rollbackPosition) {
        Map<String, String> content = new HashMap<String, String>();
        content.put("binlogSnapshotTimestamp", String.valueOf(position.getTimestamp()));
        content.put("binlogFile", rollbackPosition.getFileName());
        content.put("binlogOffest", String.valueOf(rollbackPosition.getPosition()));
        content.put("binlogMasterId", String.valueOf(rollbackPosition.getMasterId()));
        content.put("binlogTimestamp", String.valueOf(rollbackPosition.getTimestamp()));
        String timestamp = content.get("binlogTimestamp");
        String binlogSnapshotTimestamp = content.get("binlogSnapshotTimestamp");

        for (int i = 0; i < retry; i++) {
            try {
                List<MetaHistoryDO> metaHistoryDOList = metaHistoryDAO.findByTimestamp(
                    Long.valueOf(binlogSnapshotTimestamp),
                    Long.valueOf(timestamp));
                JSONObject json = new JSONObject();
                json.put("content", JSON.toJSONString(metaHistoryDOList));
                String data = ObjectUtils.toString(json.get("content"));
                JSONArray jsonArray = JSON.parseArray(data);
                for (Object jsonObj : jsonArray) {
                    JSONObject jsonData = (JSONObject)jsonObj;
                    String binlogFile = jsonData.getString("binlogFile");
                    String binlogOffest = jsonData.getString("binlogOffest");
                    String binlogMasterId = jsonData.getString("binlogMasterId");
                    String binlogTimestamp = jsonData.getString("binlogTimestamp");
                    String useSchema = jsonData.getString("useSchema");
                    String sqlData = jsonData.getString("sql");
                    BinlogPosition snapshotPosition = new BinlogPosition(binlogFile,
                        Long.valueOf(binlogOffest == null ? "0" : binlogOffest),
                        Long.valueOf(binlogMasterId == null ? "-2" : binlogMasterId),
                        Long.valueOf(binlogTimestamp == null ? "0" : binlogTimestamp));

                    // 如果是同一秒内,对比一下history的位点，如果比期望的位点要大，忽略之
                    if (snapshotPosition.getTimestamp() > rollbackPosition.getTimestamp()) {
                        continue;
                    } else if (rollbackPosition.getMasterId() == snapshotPosition.getMasterId()
                        && snapshotPosition.compareTo(rollbackPosition) > 0) {
                        continue;
                    }

                    // 记录到内存
                    if (!memoryTableMeta.apply(snapshotPosition, useSchema, sqlData)) {
                        return false;
                    }

                }

                return jsonArray.size() > 0;
            } catch (Throwable e) {

                throw new RuntimeException("apply failed", e);

            }

        }

        return false;
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

            if (sourceField.isKey() != targetField.isKey()) {
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

    public MysqlConnection getConnection() {
        return connection;
    }

    public boolean isUkDuplicateException(Throwable t) {
        if (pattern.matcher(t.getMessage()).find()) {
            // 违反外键约束时也抛出这种异常，所以这里还要判断包含字符串Duplicate entry
            return true;
        }
        return false;
    }
}
