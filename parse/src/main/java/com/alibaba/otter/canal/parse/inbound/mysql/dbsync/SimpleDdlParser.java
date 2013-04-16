package com.alibaba.otter.canal.parse.inbound.mysql.dbsync;

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
 * @author jianghang 2013-1-22 下午10:03:22
 * @version 1.0.0
 */
public class SimpleDdlParser {

    public static final String CREATE_PATTERN = "^\\s*CREATE\\s*TABLE\\s*(.*)$";
    public static final String DROP_PATTERN   = "^\\s*DROP\\s*TABLE\\s*(.*)$";
    public static final String ALERT_PATTERN  = "^\\s*ALTER\\s*TABLE\\s*(.*)$";
    public static final String TABLE_PATTERN  = "^(IF\\s*NOT\\s*EXIST\\s*)?(IF\\s*EXIST\\s*)?(`?.+?`?\\.)?(`?.+?`?[;\\(\\s]+?)?.*$"; // 采用非贪婪模式

    public static DdlResult parse(String queryString, String schmeaName) {
        DdlResult result = parse(queryString, schmeaName, ALERT_PATTERN);
        if (result != null) {
            result.setType(EventType.ALTER);
            return result;
        }

        result = parse(queryString, schmeaName, CREATE_PATTERN);
        if (result != null) {
            result.setType(EventType.CREATE);
            return result;
        }

        result = parse(queryString, schmeaName, DROP_PATTERN);
        if (result != null) {
            result.setType(EventType.ERASE);
            return result;
        }

        result = new DdlResult(schmeaName);
        result.setType(EventType.QUERY);
        return result;
    }

    private static DdlResult parse(String queryString, String schmeaName, String pattern) {
        Perl5Matcher matcher = new Perl5Matcher();
        if (matcher.matches(queryString, PatternUtils.getPattern(pattern))) {
            Perl5Matcher tableMatcher = new Perl5Matcher();
            String matchString = matcher.getMatch().group(1) + " ";
            if (tableMatcher.matches(matchString, PatternUtils.getPattern(TABLE_PATTERN))) {
                String schmeaString = tableMatcher.getMatch().group(3);
                String tableString = tableMatcher.getMatch().group(4);
                if (StringUtils.isNotEmpty(schmeaString)) {
                    // 特殊处理引号`
                    schmeaString = StringUtils.removeEnd(schmeaString, ".");
                    schmeaString = StringUtils.removeEnd(schmeaString, "`");
                    schmeaString = StringUtils.removeStart(schmeaString, "`");

                    if (StringUtils.isNotEmpty(schmeaName) && !StringUtils.equalsIgnoreCase(schmeaString, schmeaName)) {
                        return new DdlResult(schmeaName);
                    }
                } else {
                    schmeaString = schmeaName;
                }

                tableString = StringUtils.removeEnd(tableString, ";");
                tableString = StringUtils.removeEnd(tableString, "(");
                tableString = StringUtils.trim(tableString);
                // 特殊处理引号`
                tableString = StringUtils.removeEnd(tableString, "`");
                tableString = StringUtils.removeStart(tableString, "`");
                // 处理schema.table的写法
                String names[] = StringUtils.split(tableString, ".");
                if (names != null && names.length > 1) {
                    if (StringUtils.equalsIgnoreCase(schmeaString, names[0])) {
                        return new DdlResult(schmeaString, names[1]);
                    }
                } else {
                    return new DdlResult(schmeaString, names[0]);
                }
            }

            return new DdlResult(schmeaName); // 无法解析时，直接返回schmea，进行兼容处理
        }

        return null;
    }

    public static class DdlResult {

        private String    schemaName;
        private String    tableName;
        private EventType type;

        public DdlResult(){
        }

        public DdlResult(String schemaName){
            this.schemaName = schemaName;
        }

        public DdlResult(String schemaName, String tableName){
            this.schemaName = schemaName;
            this.tableName = tableName;
        }

        public String getSchemaName() {
            return schemaName;
        }

        public void setSchemaName(String schemaName) {
            this.schemaName = schemaName;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public EventType getType() {
            return type;
        }

        public void setType(EventType type) {
            this.type = type;
        }

        public String toString() {
            return "DdlResult [schemaName=" + schemaName + ", tableName=" + tableName + ", type=" + type + "]";
        }

    }
}
