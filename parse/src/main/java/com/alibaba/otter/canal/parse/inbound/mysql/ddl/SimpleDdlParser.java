package com.alibaba.otter.canal.parse.inbound.mysql.ddl;

import org.apache.commons.lang.StringUtils;
import org.apache.oro.text.regex.Perl5Matcher;

import com.alibaba.otter.canal.filter.PatternUtils;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;

/**
 * 简单的ddl解析工具类，后续可使用cobar/druid的SqlParser进行语法树解析
 * 
 * <pre>
 * 解析支持：
 * a. 带schema: retl.retl_mark
 * b. 带引号` :  `retl.retl_mark`
 * c. 存在换行符： create table \n `retl.retl_mark`
 * </pre>
 * 
 * http://dev.mysql.com/doc/refman/5.6/en/sql-syntax-data-definition.html
 * 
 * @author jianghang 2013-1-22 下午10:03:22
 * @version 1.0.0
 */
public class SimpleDdlParser {

    public static final String CREATE_PATTERN         = "^\\s*CREATE\\s*(TEMPORARY)?\\s*TABLE\\s*(.*)$";
    public static final String DROP_PATTERN           = "^\\s*DROP\\s*(TEMPORARY)?\\s*TABLE\\s*(.*)$";
    public static final String ALERT_PATTERN          = "^\\s*ALTER\\s*(IGNORE)?\\s*TABLE\\s*(.*)$";
    public static final String TRUNCATE_PATTERN       = "^\\s*TRUNCATE\\s*(TABLE)?\\s*(.*)$";
    public static final String TABLE_PATTERN          = "^(IF\\s*NOT\\s*EXISTS\\s*)?(IF\\s*EXISTS\\s*)?(`?.+?`?[;\\(\\s]+?)?.*$";         // 采用非贪婪模式
    public static final String INSERT_PATTERN         = "^\\s*(INSERT|MERGE|REPLACE)(.*)$";
    public static final String UPDATE_PATTERN         = "^\\s*UPDATE(.*)$";
    public static final String DELETE_PATTERN         = "^\\s*DELETE(.*)$";
    public static final String RENAME_PATTERN         = "^\\s*RENAME\\s+TABLE\\s+(.+?)\\s+TO\\s+(.+?)$";
    public static final String RENAME_REMNANT_PATTERN = "^\\s*(.+?)\\s+TO\\s+(.+?)$";

    /**
     * <pre>
     * CREATE [UNIQUE|FULLTEXT|SPATIAL] INDEX index_name
     *         [index_type]
     *         ON tbl_name (index_col_name,...)
     *         [algorithm_option | lock_option] ...
     *         
     * http://dev.mysql.com/doc/refman/5.6/en/create-index.html
     * </pre>
     */
    public static final String CREATE_INDEX_PATTERN   = "^\\s*CREATE\\s*(UNIQUE)?(FULLTEXT)?(SPATIAL)?\\s*INDEX\\s*(.*?)\\s+ON\\s+(.*?)$";
    public static final String DROP_INDEX_PATTERN     = "^\\s*DROP\\s*INDEX\\s*(.*?)\\s+ON\\s+(.*?)$";

    public static DdlResult parse(String queryString, String schmeaName) {
        queryString = removeComment(queryString); // 去除/* */的sql注释内容
        DdlResult result = parseDdl(queryString, schmeaName, ALERT_PATTERN, 2);
        if (result != null) {
            result.setType(EventType.ALTER);
            return result;
        }

        result = parseDdl(queryString, schmeaName, CREATE_PATTERN, 2);
        if (result != null) {
            result.setType(EventType.CREATE);
            return result;
        }

        result = parseDdl(queryString, schmeaName, DROP_PATTERN, 2);
        if (result != null) {
            result.setType(EventType.ERASE);
            return result;
        }

        result = parseDdl(queryString, schmeaName, TRUNCATE_PATTERN, 2);
        if (result != null) {
            result.setType(EventType.TRUNCATE);
            return result;
        }

        result = parseRename(queryString, schmeaName, RENAME_PATTERN);
        if (result != null) {
            result.setType(EventType.RENAME);

            String[] renameStrings = queryString.split(",");
            if (renameStrings.length > 1) {
                DdlResult lastResult = result;
                for (int i = 1; i < renameStrings.length; i++) {
                    DdlResult ddlResult = parseRename(renameStrings[i], schmeaName, RENAME_REMNANT_PATTERN);
                    ddlResult.setType(EventType.RENAME);
                    lastResult.setRenameTableResult(ddlResult);
                    lastResult = ddlResult;
                }
            }

            return result;
        }

        result = parseDdl(queryString, schmeaName, CREATE_INDEX_PATTERN, 5);
        if (result != null) {
            result.setType(EventType.CINDEX);
            return result;
        }

        result = parseDdl(queryString, schmeaName, DROP_INDEX_PATTERN, 2);
        if (result != null) {
            result.setType(EventType.DINDEX);
            return result;
        }

        result = new DdlResult(schmeaName);
        if (isDml(queryString, INSERT_PATTERN)) {
            result.setType(EventType.INSERT);
            return result;
        }

        if (isDml(queryString, UPDATE_PATTERN)) {
            result.setType(EventType.UPDATE);
            return result;
        }

        if (isDml(queryString, DELETE_PATTERN)) {
            result.setType(EventType.DELETE);
            return result;
        }

        result.setType(EventType.QUERY);
        return result;
    }

    private static DdlResult parseDdl(String queryString, String schmeaName, String pattern, int index) {
        Perl5Matcher matcher = new Perl5Matcher();
        if (matcher.matches(queryString, PatternUtils.getPattern(pattern))) {
            DdlResult result = parseTableName(matcher.getMatch().group(index), schmeaName);
            return result != null ? result : new DdlResult(schmeaName); // 无法解析时，直接返回schmea，进行兼容处理
        }

        return null;
    }

    private static boolean isDml(String queryString, String pattern) {
        Perl5Matcher matcher = new Perl5Matcher();
        if (matcher.matches(queryString, PatternUtils.getPattern(pattern))) {
            return true;
        } else {
            return false;
        }
    }

    private static DdlResult parseRename(String queryString, String schmeaName, String pattern) {
        Perl5Matcher matcher = new Perl5Matcher();
        if (matcher.matches(queryString, PatternUtils.getPattern(pattern))) {
            DdlResult orign = parseTableName(matcher.getMatch().group(1), schmeaName);
            DdlResult target = parseTableName(matcher.getMatch().group(2), schmeaName);
            if (orign != null && target != null) {
                return new DdlResult(target.getSchemaName(),
                    target.getTableName(),
                    orign.getSchemaName(),
                    orign.getTableName());
            }
        }

        return null;
    }

    private static DdlResult parseTableName(String matchString, String schmeaName) {
        Perl5Matcher tableMatcher = new Perl5Matcher();
        matchString = matchString + " ";
        if (tableMatcher.matches(matchString, PatternUtils.getPattern(TABLE_PATTERN))) {
            String tableString = tableMatcher.getMatch().group(3);
            if (StringUtils.isEmpty(tableString)) {
                return null;
            }

            tableString = StringUtils.removeEnd(tableString, ";");
            tableString = StringUtils.removeEnd(tableString, "(");
            tableString = StringUtils.trim(tableString);
            // 特殊处理引号`
            tableString = removeEscape(tableString);
            // 处理schema.table的写法
            String names[] = StringUtils.split(tableString, ".");
            if (names.length == 0) {
                return null;
            }

            if (names != null && names.length > 1) {
                return new DdlResult(removeEscape(names[0]), removeEscape(names[1]));
            } else {
                return new DdlResult(schmeaName, removeEscape(names[0]));
            }
        }

        return null;
    }

    private static String removeEscape(String str) {
        String result = StringUtils.removeEnd(str, "`");
        result = StringUtils.removeStart(result, "`");
        return result;
    }

    private static String removeComment(String sql) {
        if (sql == null) {
            return null;
        }

        String start = "/*";
        String end = "*/";
        while (true) {
            // 循环找到所有的注释
            int index0 = sql.indexOf(start);
            if (index0 == -1) {
                return sql;
            }
            int index1 = sql.indexOf(end, index0);
            if (index1 == -1) {
                return sql;
            }
            StringBuilder sb = new StringBuilder();
            sb.append(sql.substring(0, index0));
            sb.append(" ");
            sb.append(sql.substring(index1 + end.length()));
            sql = sb.toString();
        }
    }
}
