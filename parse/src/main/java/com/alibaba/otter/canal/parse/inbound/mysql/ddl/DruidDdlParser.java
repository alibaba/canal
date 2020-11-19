package com.alibaba.otter.canal.parse.inbound.mysql.ddl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableAddConstraint;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableAddIndex;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropConstraint;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropIndex;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropKey;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableRename;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLConstraint;
import com.alibaba.druid.sql.ast.statement.SQLCreateDatabaseStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateIndexStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropDatabaseStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropIndexStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTruncateStatement;
import com.alibaba.druid.sql.ast.statement.SQLUnique;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement.Item;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.util.JdbcConstants;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;

/**
 * @author agapple 2017年7月27日 下午4:05:34
 * @since 1.0.25
 */
public class DruidDdlParser {

    public static List<DdlResult> parse(String queryString, String schmeaName) {
        List<SQLStatement> stmtList = null;
        try {
            stmtList = SQLUtils.parseStatements(queryString, JdbcConstants.MYSQL, false);
        } catch (ParserException e) {
            // 可能存在一些SQL是不支持的，比如存储过程
            DdlResult ddlResult = new DdlResult();
            ddlResult.setType(EventType.QUERY);
            return Arrays.asList(ddlResult);
        }

        List<DdlResult> ddlResults = new ArrayList<>();
        for (SQLStatement statement : stmtList) {
            if (statement instanceof SQLCreateTableStatement) {
                DdlResult ddlResult = new DdlResult();
                SQLCreateTableStatement createTable = (SQLCreateTableStatement) statement;
                processName(ddlResult, schmeaName, createTable.getName(), false);
                ddlResult.setType(EventType.CREATE);
                ddlResults.add(ddlResult);
            } else if (statement instanceof SQLAlterTableStatement) {
                SQLAlterTableStatement alterTable = (SQLAlterTableStatement) statement;
                for (SQLAlterTableItem item : alterTable.getItems()) {
                    if (item instanceof SQLAlterTableRename) {
                        DdlResult ddlResult = new DdlResult();
                        processName(ddlResult, schmeaName, alterTable.getName(), true);
                        processName(ddlResult, schmeaName, ((SQLAlterTableRename) item).getToName(), false);
                        ddlResult.setType(EventType.RENAME);
                        ddlResults.add(ddlResult);
                    } else if (item instanceof SQLAlterTableAddIndex) {
                        DdlResult ddlResult = new DdlResult();
                        processName(ddlResult, schmeaName, alterTable.getName(), false);
                        ddlResult.setType(EventType.CINDEX);
                        ddlResults.add(ddlResult);
                    } else if (item instanceof SQLAlterTableDropIndex || item instanceof SQLAlterTableDropKey) {
                        DdlResult ddlResult = new DdlResult();
                        processName(ddlResult, schmeaName, alterTable.getName(), false);
                        ddlResult.setType(EventType.DINDEX);
                        ddlResults.add(ddlResult);
                    } else if (item instanceof SQLAlterTableAddConstraint) {
                        DdlResult ddlResult = new DdlResult();
                        processName(ddlResult, schmeaName, alterTable.getName(), false);
                        SQLConstraint constraint = ((SQLAlterTableAddConstraint) item).getConstraint();
                        if (constraint instanceof SQLUnique) {
                            ddlResult.setType(EventType.CINDEX);
                            ddlResults.add(ddlResult);
                        }
                    } else if (item instanceof SQLAlterTableDropConstraint) {
                        DdlResult ddlResult = new DdlResult();
                        processName(ddlResult, schmeaName, alterTable.getName(), false);
                        ddlResult.setType(EventType.DINDEX);
                        ddlResults.add(ddlResult);
                    } else {
                        DdlResult ddlResult = new DdlResult();
                        processName(ddlResult, schmeaName, alterTable.getName(), false);
                        ddlResult.setType(EventType.ALTER);
                        ddlResults.add(ddlResult);
                    }
                }
            } else if (statement instanceof SQLDropTableStatement) {
                SQLDropTableStatement dropTable = (SQLDropTableStatement) statement;
                for (SQLExprTableSource tableSource : dropTable.getTableSources()) {
                    DdlResult ddlResult = new DdlResult();
                    processName(ddlResult, schmeaName, tableSource.getExpr(), false);
                    ddlResult.setType(EventType.ERASE);
                    ddlResults.add(ddlResult);
                }
            } else if (statement instanceof SQLCreateIndexStatement) {
                SQLCreateIndexStatement createIndex = (SQLCreateIndexStatement) statement;
                SQLTableSource tableSource = createIndex.getTable();
                DdlResult ddlResult = new DdlResult();
                processName(ddlResult, schmeaName, ((SQLExprTableSource) tableSource).getExpr(), false);
                ddlResult.setType(EventType.CINDEX);
                ddlResults.add(ddlResult);
            } else if (statement instanceof SQLDropIndexStatement) {
                SQLDropIndexStatement dropIndex = (SQLDropIndexStatement) statement;
                SQLExprTableSource tableSource = dropIndex.getTableName();
                DdlResult ddlResult = new DdlResult();
                processName(ddlResult, schmeaName, tableSource.getExpr(), false);
                ddlResult.setType(EventType.DINDEX);
                ddlResults.add(ddlResult);
            } else if (statement instanceof SQLTruncateStatement) {
                SQLTruncateStatement truncate = (SQLTruncateStatement) statement;
                for (SQLExprTableSource tableSource : truncate.getTableSources()) {
                    DdlResult ddlResult = new DdlResult();
                    processName(ddlResult, schmeaName, tableSource.getExpr(), false);
                    ddlResult.setType(EventType.TRUNCATE);
                    ddlResults.add(ddlResult);
                }
            } else if (statement instanceof MySqlRenameTableStatement) {
                MySqlRenameTableStatement rename = (MySqlRenameTableStatement) statement;
                for (Item item : rename.getItems()) {
                    DdlResult ddlResult = new DdlResult();
                    processName(ddlResult, schmeaName, item.getName(), true);
                    processName(ddlResult, schmeaName, item.getTo(), false);
                    ddlResult.setType(EventType.RENAME);
                    ddlResults.add(ddlResult);
                }
            } else if (statement instanceof SQLInsertStatement) {
                DdlResult ddlResult = new DdlResult();
                SQLInsertStatement insert = (SQLInsertStatement) statement;
                processName(ddlResult, schmeaName, insert.getTableName(), false);
                ddlResult.setType(EventType.INSERT);
                ddlResults.add(ddlResult);
            } else if (statement instanceof SQLUpdateStatement) {
                DdlResult ddlResult = new DdlResult();
                SQLUpdateStatement update = (SQLUpdateStatement) statement;
                // 拿到的表名可能为null,比如update a,b set a.id=x
                processName(ddlResult, schmeaName, update.getTableName(), false);
                ddlResult.setType(EventType.UPDATE);
                ddlResults.add(ddlResult);
            } else if (statement instanceof SQLDeleteStatement) {
                DdlResult ddlResult = new DdlResult();
                SQLDeleteStatement delete = (SQLDeleteStatement) statement;
                // 拿到的表名可能为null,比如delete a,b from a where a.id = b.id
                processName(ddlResult, schmeaName, delete.getTableName(), false);
                ddlResult.setType(EventType.DELETE);
                ddlResults.add(ddlResult);
            } else if (statement instanceof SQLCreateDatabaseStatement) {
                SQLCreateDatabaseStatement create = (SQLCreateDatabaseStatement) statement;
                DdlResult ddlResult = new DdlResult();
                ddlResult.setType(EventType.QUERY);
                processName(ddlResult, create.getDatabaseName(), null, false);
                ddlResults.add(ddlResult);
            } else if (statement instanceof SQLDropDatabaseStatement) {
                SQLDropDatabaseStatement drop = (SQLDropDatabaseStatement) statement;
                DdlResult ddlResult = new DdlResult();
                ddlResult.setType(EventType.QUERY);
                processName(ddlResult, drop.getDatabaseName(), null, false);
                ddlResults.add(ddlResult);
            }
        }

        return ddlResults;
    }

    private static void processName(DdlResult ddlResult, String schema, SQLExpr sqlName, boolean isOri) {
        if (sqlName == null) {
            if (StringUtils.isNotBlank(schema)) {
                ddlResult.setSchemaName(schema);
            }
            return;
        }

        String table = null;
        if (sqlName instanceof SQLPropertyExpr) {
            SQLIdentifierExpr owner = (SQLIdentifierExpr) ((SQLPropertyExpr) sqlName).getOwner();
            schema = unescapeName(owner.getName());
            table = unescapeName(((SQLPropertyExpr) sqlName).getName());
        } else if (sqlName instanceof SQLIdentifierExpr) {
            table = unescapeName(((SQLIdentifierExpr) sqlName).getName());
        }

        if (isOri) {
            ddlResult.setOriSchemaName(schema);
            ddlResult.setOriTableName(table);
        } else {
            ddlResult.setSchemaName(schema);
            ddlResult.setTableName(table);
        }
    }

    public static String unescapeName(String name) {
        if (name != null && name.length() > 2) {
            char c0 = name.charAt(0);
            char x0 = name.charAt(name.length() - 1);
            if ((c0 == '"' && x0 == '"') || (c0 == '`' && x0 == '`')) {
                return name.substring(1, name.length() - 1);
            }
        }

        return name;
    }

    public static String unescapeQuotaName(String name) {
        if (name != null && name.length() > 2) {
            char c0 = name.charAt(0);
            char x0 = name.charAt(name.length() - 1);
            if (c0 == '\'' && x0 == '\'') {
                return name.substring(1, name.length() - 1);
            }
        }

        return name;
    }

}
