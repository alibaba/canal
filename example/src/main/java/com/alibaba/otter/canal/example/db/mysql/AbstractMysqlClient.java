package com.alibaba.otter.canal.example.db.mysql;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.example.db.AbstractDbClient;
import com.alibaba.otter.canal.example.db.dialect.DbDialect;
import com.alibaba.otter.canal.example.db.dialect.mysql.MysqlDialect;
import com.alibaba.otter.canal.example.db.dialect.mysql.MysqlSqlTemplate;
import com.alibaba.otter.canal.example.db.dialect.SqlTemplate;
import com.alibaba.otter.canal.example.db.utils.SqlUtils;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import org.apache.commons.lang.StringUtils;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Table;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.StatementCreatorUtils;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobCreator;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public abstract class AbstractMysqlClient extends AbstractDbClient {

    private DataSource dataSource;

    private DbDialect dbDialect;
    private SqlTemplate sqlTemplate;

    protected Integer execute(final CanalEntry.Header header, final List<CanalEntry.Column> columns) {
        final String sql = getSql(header, columns);
        final LobCreator lobCreator = dbDialect.getLobHandler().getLobCreator();
        dbDialect.getTransactionTemplate().execute(new TransactionCallback() {

            public Object doInTransaction(TransactionStatus status) {
                try {
                    JdbcTemplate template = dbDialect.getJdbcTemplate();
                    int affect = template.update(sql, new PreparedStatementSetter() {

                        public void setValues(PreparedStatement ps) throws SQLException {
                            doPreparedStatement(ps, dbDialect, lobCreator, header, columns);
                        }
                    });
                    return affect;
                } finally {
                    lobCreator.close();
                }
            }
        });
        return 0;
    }

    private String getSql(CanalEntry.Header header, List<CanalEntry.Column> columns) {
        List<String> pkNames = new ArrayList<>();
        List<String> colNames = new ArrayList<>();
        for (CanalEntry.Column column : columns) {
            if (column.getIsKey()) {
                pkNames.add(column.getName());
            } else {
                colNames.add(column.getName());
            }
        }
        String sql = "";
        CanalEntry.EventType eventType = header.getEventType();
        switch (eventType) {
            case INSERT:
                sql = sqlTemplate.getInsertSql(header.getSchemaName(), header.getTableName(), pkNames.toArray(new String[]{}), colNames.toArray(new String[]{}));
                break;
            case UPDATE:
                sql = sqlTemplate.getUpdateSql(header.getSchemaName(), header.getTableName(), pkNames.toArray(new String[]{}), colNames.toArray(new String[]{}));
                break;
            case DELETE:
                sql = sqlTemplate.getDeleteSql(header.getSchemaName(), header.getTableName(), pkNames.toArray(new String[]{}));
        }
        logger.info("Execute sql: {}", sql);
        return sql;
    }

    private void doPreparedStatement(PreparedStatement ps, DbDialect dbDialect, LobCreator lobCreator,
                                     CanalEntry.Header header, List<CanalEntry.Column> columns) throws SQLException {

        List<CanalEntry.Column> rebuildColumns = new ArrayList<>(columns.size());

        List<CanalEntry.Column> keyColumns = new ArrayList<>(columns.size());
        List<CanalEntry.Column> notKeyColumns = new ArrayList<>(columns.size());
        for (CanalEntry.Column column : columns) {
            if (column.getIsKey()) {
                keyColumns.add(column);
            } else {
                notKeyColumns.add(column);
            }
        }
        CanalEntry.EventType eventType = header.getEventType();
        switch (eventType) {
            case INSERT:
            case UPDATE:
                // insert/update语句对应的字段数序都是将主键排在后面
                rebuildColumns.addAll(notKeyColumns);
                rebuildColumns.addAll(keyColumns);
                break;
            case DELETE:
                rebuildColumns.addAll(keyColumns);
        }

        // 获取一下当前字段名的数据是否必填
        Table table = dbDialect.findTable(header.getSchemaName(), header.getTableName());
        Map<String, Boolean> isRequiredMap = new HashMap();
        for (Column tableColumn : table.getColumns()) {
            isRequiredMap.put(StringUtils.lowerCase(tableColumn.getName()), tableColumn.isRequired());
        }

        List<Object> values = new ArrayList<>(rebuildColumns.size());
        for (int i = 0; i < rebuildColumns.size(); i++) {
            int paramIndex = i + 1;
            CanalEntry.Column column = rebuildColumns.get(i);
            int sqlType = column.getSqlType();

            Boolean isRequired = isRequiredMap.get(StringUtils.lowerCase(column.getName()));
            if (isRequired == null) {
                // 清理一下目标库的表结构,二次检查一下
                table = dbDialect.findTable(header.getSchemaName(), header.getTableName());

                isRequiredMap = new HashMap<>();
                for (Column tableColumn : table.getColumns()) {
                    isRequiredMap.put(StringUtils.lowerCase(tableColumn.getName()), tableColumn.isRequired());
                }

                isRequired = isRequiredMap.get(StringUtils.lowerCase(column.getName()));
                if (isRequired == null) {
                    throw new CanalClientException(String.format("column name %s is not found in Table[%s]",
                            column.getName(),
                            table.toString()));
                }
            }

            Object param;
            if (sqlType == Types.TIME || sqlType == Types.TIMESTAMP || sqlType == Types.DATE) {
                // 解决mysql的0000-00-00 00:00:00问题，直接依赖mysql
                // driver进行处理，如果转化为Timestamp会出错
                param = column.getValue();
                if (param instanceof String && StringUtils.isEmpty(String.valueOf(param))) {
                    param = null;
                }
            } else {
                param = SqlUtils.stringToSqlValue(column.getValue(),
                        sqlType,
                        isRequired,
                        column.getIsNull());
            }

            try {
                switch (sqlType) {
                    case Types.CLOB:
                        lobCreator.setClobAsString(ps, paramIndex, (String) param);
                        break;
                    case Types.BLOB:
                        lobCreator.setBlobAsBytes(ps, paramIndex, (byte[]) param);
                        break;
                    case Types.TIME:
                    case Types.TIMESTAMP:
                    case Types.DATE:
                        ps.setObject(paramIndex, param);
                        break;
                    case Types.BIT:
                        StatementCreatorUtils.setParameterValue(ps, paramIndex, Types.DECIMAL, null, param);
                        break;
                    default:
                        StatementCreatorUtils.setParameterValue(ps, paramIndex, sqlType, null, param);
                        break;
                }
                values.add(param);
            } catch (SQLException ex) {
                logger.error("## SetParam error , [sqltype={}, value={}]",
                        new Object[]{sqlType, param});
                throw ex;
            }
        }
        logger.info("## sql values: {}", JSON.toJSONString(values));
    }

    @Override
    public void afterPropertiesSet() {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        DefaultLobHandler lobHandler = new DefaultLobHandler();
        lobHandler.setStreamAsLob(true);
        dbDialect = new MysqlDialect(jdbcTemplate, lobHandler);
        sqlTemplate = new MysqlSqlTemplate();
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }
}
