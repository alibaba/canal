package com.alibaba.otter.canal.client.adapter.phoenix.support;

import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLDataTypeImpl;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import org.apache.commons.lang.StringUtils;

/**
 * Phoenix类型   此类型完全新增
 */
public class TypeUtil {

    private static String joinArgs(String type, Object[] args) {
        if (args.length > 0) {
            return type + "(" + StringUtils.join(args, ",") + ")";
        }
        return type;
    }

    /**
     * 根据SQL的定义返回Phoenix的类型定义
     * @see "https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-type-conversions.html"
     * @param definition SQL的字段定义
     * @param limit 是否启用字段长度限制
     * @return Phoenix字段类型定义
     */
    public static String getPhoenixType(SQLColumnDefinition definition, boolean limit) {
        if (definition == null) return "VARCHAR";
        SQLDataType sqlDataType = definition.getDataType();
        SQLDataTypeImpl sqlDataType1 = sqlDataType instanceof SQLDataTypeImpl ? (SQLDataTypeImpl)sqlDataType : null;
        boolean isUnsigned = sqlDataType1 != null && sqlDataType1.isUnsigned();

        return getPhoenixType(sqlDataType.getName().toUpperCase(), sqlDataType.getArguments().toArray(), isUnsigned, limit);
    }

    //MySQL类型和Phoenix类型一一对应
    public static String getPhoenixType(String name, Object[] args, boolean isUnsigned, boolean limit) {
        switch (name) {
            case "BIT":
                if (limit) {
                    return joinArgs("BINARY", args);
                }
                return "BINARY";
            case "TINYINT":
                if (isUnsigned) {
                    return "UNSIGNED_TINYINT";
                }
                return "TINYINT";
            case "BOOLEAN":
            case "BOOL":
                return "BOOLEAN";
            case "SMALLINT":
                if (isUnsigned) {
                    return "UNSIGNED_SMALLINT";
                }
                return "SMALLINT";
            case "MEDIUMINT":
                return "INTEGER";
            case "INT":
            case "INTEGER":
                if (isUnsigned) {
                    return "UNSIGNED_INT";
                }
                return "INTEGER";
            case "BIGINT":
                if (isUnsigned) {
                    return "UNSIGNED_LONG";
                }
                return "BIGINT";
            case "FLOAT":
                if (isUnsigned) {
                    return "UNSIGNED_FLOAT";
                }
                return "FLOAT";
            case "DOUBLE":
                if (isUnsigned) {
                    return "UNSIGNED_DOUBLE";
                }
                return "DOUBLE";
            case "DECIMAL":
                if (limit) {
                    return joinArgs("DECIMAL", args);
                }
                return "DECIMAL";
            case "DATE":
                if (isUnsigned) {
                    return "UNSIGNED_DATE";
                }
                return "DATE";
            case "DATETIME":
            case "TIMESTAMP":
                if (isUnsigned) {
                    return "UNSIGNED_TIMESTAMP";
                }
                return "TIMESTAMP";
            case "TIME":
                if (isUnsigned) {
                    return "UNSIGNED_TIME";
                }
                return "TIME";
            case "YEAR":
                return "INTEGER";
            case "CHAR":
                if (limit) {
                    return joinArgs(name, args);
                }
                return "VARCHAR";
            case "VARCHAR":
                if (limit) {
                    return joinArgs(name, args);
                }
                return "VARCHAR";
            case "BINARY":
                if (limit) {
                    return joinArgs(name, args);
                }
                return "VARBINARY";
            case "VARBINARY":
                return "VARBINARY";
            case "TINYBLOB":
                return "VARBINARY";
            case "TINYTEXT":
                return "VARCHAR";
            case "BLOB":
                return "VARBINARY";
            case "TEXT":
                return "VARCHAR";
            case "MEDIUMBLOB":
                return "VARBINARY";
            case "MEDIUMTEXT":
                return "VARCHAR";
            case "LONGBLOB":
                return "VARBINARY";
            case "LONGTEXT":
                return "VARCHAR";
            case "ENUM":
            case "SET":
                return "VARCHAR";
        }
        return "VARCHAR";
    }
}
