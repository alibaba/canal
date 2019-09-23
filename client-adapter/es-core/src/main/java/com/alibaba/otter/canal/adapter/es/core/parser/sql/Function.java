package com.alibaba.otter.canal.adapter.es.core.parser.sql;

import java.sql.Timestamp;
import java.util.*;

import com.alibaba.fastsql.sql.ast.expr.*;
import com.alibaba.otter.canal.adapter.es.core.util.StringUtils;
import com.alibaba.otter.canal.adapter.es.core.util.Util;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.PeriodType;

import com.alibaba.fastsql.sql.ast.SQLExpr;

public class Function {

    private static final HashMap<String, Integer> FUNCTIONS = new HashMap<>(256);

    private static final String[] WEEK_DAYS = new String[]{"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"};
    private static final String[] MONTH_NAMES = new String[]{"January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"};

    private static void addFunction(String functionName, int functionType) {
        FUNCTIONS.put(functionName, functionType);
    }

    private static int getFunction(String functionName) {
        Integer func = FUNCTIONS.get(functionName);
        if (func == null) {
            return -1;
        } else {
            return func;
        }
    }

    static {
        addFunction("ABS", 0);
        addFunction("ACOS", 1);
        addFunction("ASIN", 2);
        addFunction("ATAN", 3);
        addFunction("ATAN2", 4);
        addFunction("CEILING", 8);
        addFunction("COS", 9);
        addFunction("COT", 10);
        addFunction("DEGREES", 11);
        addFunction("EXP", 12);
        addFunction("FLOOR", 13);
        addFunction("LOG", 14);
        addFunction("LOG10", 15);
        addFunction("MOD", 16);
        addFunction("PI", 17);
        addFunction("POWER", 18);
        addFunction("RADIANS", 19);
        addFunction("ROUND", 21);
        addFunction("SIGN", 23);
        addFunction("SIN", 24);
        addFunction("SQRT", 25);
        addFunction("TAN", 26);
        addFunction("TRUNCATE", 27);

        addFunction("ASCII", 50);
        addFunction("BIT_LENGTH", 51);
        addFunction("CHAR", 52);
        addFunction("CHAR_LENGTH", 53);
        addFunction("CONCAT", 54);
        addFunction("CONCAT_WS", 55);
        addFunction("INSTR", 58);
        addFunction("LCASE", 59);
        addFunction("LEFT", 60);
        addFunction("LENGTH", 61);
        addFunction("LOCATE", 62);
        addFunction("LTRIM", 63);
        addFunction("OCTET_LENGTH", 64);
        addFunction("REPEAT", 66);
        addFunction("REPLACE", 67);
        addFunction("RIGHT", 68);
        addFunction("RTRIM", 69);
        addFunction("SPACE", 71);
        addFunction("SUBSTR", 72);
        addFunction("SUBSTRING", 73);
        addFunction("UCASE", 74);
        addFunction("LOWER", 75);
        addFunction("UPPER", 76);
        addFunction("POSITION", 77);
        addFunction("TRIM", 78);
        addFunction("RPAD", 90);
        addFunction("LPAD", 91);
        addFunction("TO_CHAR", 93);

        addFunction("DATE_ADD", 105);
        addFunction("DATEDIFF", 106);
        addFunction("DAYNAME", 107);
        addFunction("DAYOFMONTH", 108);
        addFunction("DAYOFWEEK", 109);
        addFunction("DAYOFYEAR", 110);
        addFunction("HOUR", 111);
        addFunction("MINUTE", 112);
        addFunction("MONTH", 113);
        addFunction("MONTHNAME", 114);
        addFunction("QUARTER", 115);
        addFunction("SECOND", 116);
        addFunction("WEEK", 117);
        addFunction("YEAR", 118);

        addFunction("IFNULL", 200);
        addFunction("NULLIF", 205);
        addFunction("ISNULL", 206);
    }

    public static final int ABS = 0, ACOS = 1, ASIN = 2, ATAN = 3, ATAN2 = 4, CEILING = 8, COS = 9, COT = 10,
            DEGREES = 11, EXP = 12, FLOOR = 13, LOG = 14, LOG10 = 15, MOD = 16, PI = 17, POWER = 18, RADIANS = 19,
            ROUND = 21, SIGN = 23, SIN = 24, SQRT = 25, TAN = 26, TRUNCATE = 27;

    public static final int ASCII = 50, BIT_LENGTH = 51, CHAR = 52, CHAR_LENGTH = 53, CONCAT = 54, CONCAT_WS = 55,
            INSTR = 58, LCASE = 59, LEFT = 60, LENGTH = 61, LOCATE = 62, LTRIM = 63, OCTET_LENGTH = 64, REPEAT = 66,
            REPLACE = 67, RIGHT = 68, RTRIM = 69, SPACE = 71, SUBSTR = 72, SUBSTRING = 73, UCASE = 74, LOWER = 75,
            UPPER = 76, POSITION = 77, TRIM = 78, RPAD = 90, LPAD = 91, TO_CHAR = 93;

    public static final int DATE_ADD = 105, DATEDIFF = 106, DAYNAME = 107, DAYOFMONTH = 108, DAYOFWEEK = 109,
            DAYOFYEAR = 110, HOUR = 111, MINUTE = 112, MONTH = 113, MONTHNAME = 114, QUARTER = 115, SECOND = 116,
            WEEK = 117, YEAR = 118;

    public static final int IFNULL = 200, NULLIF = 205, ISNULL = 206;


    public static void setFieldItem(SQLExpr expr, SqlItem.FieldItem fieldItem) {
        if (expr instanceof SQLCaseExpr) {
            SQLCaseExpr ce = (SQLCaseExpr) expr;
            if (ce.getValueExpr() != null) {
                // CASE xxx WHEN ... THEN ...
                setFieldItem(ce.getValueExpr(), fieldItem);
            }
            for (SQLCaseExpr.Item item : ce.getItems()) {
                setFieldItem(item.getConditionExpr(), fieldItem);
                setFieldItem(item.getValueExpr(), fieldItem);
            }
            setFieldItem(ce.getElseExpr(), fieldItem);
        } else if (expr instanceof SQLBinaryOpExpr) {
            //基本二元操作
            SQLBinaryOpExpr boe = (SQLBinaryOpExpr) expr;
            SQLExpr lexpr = boe.getLeft();
            setFieldItem(lexpr, fieldItem);
            SQLExpr rexpr = boe.getRight();
            setFieldItem(rexpr, fieldItem);

            switch (boe.getOperator()) {
                case GreaterThan:
                case GreaterThanOrEqual:
                case LessThan:
                case LessThanOrEqual:
                case NotEqual:
                case LessThanOrGreater:
                case Multiply:
                case DIV:
                case Divide:
                case Modulus:
                case Mod:
                case Add:
                case Subtract:
                case Equality:
                    break;
                default:
                    fieldItem.setSimple(false);
                    break;
            }
        } else if (expr instanceof SQLMethodInvokeExpr) {
            SQLMethodInvokeExpr mie = (SQLMethodInvokeExpr) expr;
            List<SQLExpr> args0 = mie.getArguments();
            for (SQLExpr arg : args0) {
                setFieldItem(arg, fieldItem);
            }
            //判断函数是否可操作
            String methodName = mie.getMethodName().toUpperCase();
            int method = getFunction(methodName);
            if (method == -1) {
                fieldItem.setSimple(false);
            }
        } else if (expr instanceof SQLIdentifierExpr) {
            // 无owner
            SQLIdentifierExpr ie = (SQLIdentifierExpr) expr;
            SqlItem.ColumnItem columnItem = new SqlItem.ColumnItem();
            columnItem.setOwner("");
            columnItem.setColumnName(Util.cleanColumn(ie.getName()));
            fieldItem.getColumnItems().add(columnItem);
            fieldItem.getOwners().add("");
            if (fieldItem.getOwners().size() > 1) {
                fieldItem.setSimple(false);
            }
            return;
        } else if (expr instanceof SQLPropertyExpr) {
            // 有owner
            SQLPropertyExpr pe = (SQLPropertyExpr) expr;
            SqlItem.ColumnItem columnItem = new SqlItem.ColumnItem();
            String owner = pe.getOwner().toString();
            columnItem.setOwner(pe.getOwner().toString());
            columnItem.setColumnName(Util.cleanColumn(pe.getName()));
            fieldItem.getColumnItems().add(columnItem);
            fieldItem.getOwners().add(owner);
            if (fieldItem.getOwners().size() > 1) {
                fieldItem.setSimple(false);
            }
            return;
        }
        fieldItem.setRaw(false);
    }

    public static Object getValue(SQLExpr expr, Map<String, Object> args) {
        Object res = null;
        if (expr instanceof SQLCharExpr) {
            // 字符串常量
            SQLCharExpr ce = (SQLCharExpr) expr;
            res = ce.getText();
        } else if (expr instanceof SQLIntegerExpr) {
            // 整型常量
            SQLIntegerExpr ie = (SQLIntegerExpr) expr;
            res = ie.getNumber();
        } else if (expr instanceof SQLNumberExpr) {
            // 数字常量
            SQLNumberExpr ne = (SQLNumberExpr) expr;
            res = ne.getNumber();
        } else if (expr instanceof SQLCaseExpr) {
            SQLCaseExpr ce = (SQLCaseExpr) expr;
            if (ce.getValueExpr() != null) {
                // CASE xxx WHEN ... THEN ...
                Object v0 = getValue(ce.getValueExpr(), args);
                boolean matched = false;
                for (SQLCaseExpr.Item item : ce.getItems()) {
                    SQLExpr condExpr = item.getConditionExpr();
                    Object condVal = getValue(condExpr, args);
                    Integer compRes = compareValues(v0, condVal);
                    if (compRes != null && compRes == 0) {
                        res = getValue(item.getValueExpr(), args);
                        matched = true;
                        break;
                    }
                }
                if (!matched) {
                    res = getValue(ce.getElseExpr(), args);
                }
            } else {
                // CASE WHEN xxx=... THEN ...
                boolean matched = false;
                for (SQLCaseExpr.Item item : ce.getItems()) {
                    Byte compared = (Byte) getValue(item.getConditionExpr(), args);
                    if (compared != null && compared == 1) {
                        res = getValue(item.getValueExpr(), args);
                        matched = true;
                        break;
                    }
                }
                if (!matched) {
                    res = getValue(ce.getElseExpr(), args);
                }
            }
        } else if (expr instanceof SQLBinaryOpExpr) {
            // 基本二元操作
            SQLBinaryOpExpr boe = (SQLBinaryOpExpr) expr;
            SQLExpr lexpr = boe.getLeft();
            SQLExpr rexpr = boe.getRight();
            if (!(lexpr instanceof SQLNullExpr) && !(rexpr instanceof SQLNullExpr)) {
                Object lval = getValue(lexpr, args);
                Object rval = getValue(rexpr, args);
                // compare
                Integer compared = compareValues(lval, rval);
                if (compared != null) {
                    switch (boe.getOperator()) {
                        case GreaterThan:
                            res = compared > 0 ? (byte) 1 : (byte) 0;
                            break;
                        case GreaterThanOrEqual:
                            res = compared >= 0 ? (byte) 1 : (byte) 0;
                            break;
                        case LessThan:
                            res = compared < 0 ? (byte) 1 : (byte) 0;
                            break;
                        case LessThanOrEqual:
                            res = compared <= 0 ? (byte) 1 : (byte) 0;
                            break;
                        case NotEqual:
                        case LessThanOrGreater:
                            res = compared != 0 ? (byte) 1 : (byte) 0;
                            break;
                        default:
                            break;
                    }
                }
                if (res == null) {
                    // binary operation
                    NumValue lnum = NumValue.valueOf(lval);
                    NumValue rnum = NumValue.valueOf(rval);
                    if (lnum != null && rnum != null) {
                        switch (boe.getOperator()) {
                            case Multiply:
                                res = lnum.multiply(rnum).getValue();
                                break;
                            case DIV:
                            case Divide:
                                res = lnum.toDecimal().divide(rnum).getValue();
                                break;
                            case Modulus:
                            case Mod:
                                res = lnum.mod(rnum).getValue();
                                break;
                            case Add:
                                res = lnum.add(rnum).getValue();
                                break;
                            case Subtract:
                                res = lnum.subtract(rnum).getValue();
                                break;
                            case Equality:
                                res = lnum.compareTo(rnum) == 0 ? (byte) 1 : (byte) 0;
                                break;
                            default:
                                break;
                        }
                    }
                }
            }
        } else if (expr instanceof SQLMethodInvokeExpr) {
            SQLMethodInvokeExpr mie = (SQLMethodInvokeExpr) expr;
            String methodName = mie.getMethodName().toUpperCase();
            int method = getFunction(methodName);
            List<SQLExpr> args0 = mie.getArguments();

            switch (method) {
                case ABS: {
                    SQLExpr argExpr = args0.get(0);
                    Object v0 = getValue(argExpr, args);
                    if (v0 != null) {
                        res = NumValue.valueOf(v0).abs().getValue();
                    }
                    break;
                }
                case ACOS: {
                    SQLExpr argExpr = args0.get(0);
                    Object v0 = getValue(argExpr, args);
                    if (v0 != null) {
                        res = Math.acos(NumValue.valueOf(v0).doubleValue());
                    }
                    break;
                }
                case ASIN: {
                    SQLExpr argExpr = args0.get(0);
                    Object v0 = getValue(argExpr, args);
                    if (v0 != null) {
                        res = Math.asin(NumValue.valueOf(v0).doubleValue());
                    }
                    break;
                }
                case ATAN: {
                    SQLExpr argExpr = args0.get(0);
                    Object v0 = getValue(argExpr, args);
                    if (v0 != null) {
                        res = Math.atan(NumValue.valueOf(v0).doubleValue());
                    }
                    break;
                }
                case ATAN2: {
                    Object v0 = getValue(args0.get(0), args);
                    Object v1 = getValue(args0.get(1), args);
                    if (v0 != null && v1 != null) {
                        res = Math.atan2(NumValue.valueOf(v0).doubleValue(), NumValue.valueOf(v1).doubleValue());
                    }
                    break;
                }
                case CEILING: {
                    SQLExpr argExpr = args0.get(0);
                    Object v0 = getValue(argExpr, args);
                    if (v0 != null) {
                        res = Math.ceil(NumValue.valueOf(v0).doubleValue());
                    }
                    break;
                }
                case COS: {
                    SQLExpr argExpr = args0.get(0);
                    Object v0 = getValue(argExpr, args);
                    if (v0 != null) {
                        res = Math.cos(NumValue.valueOf(v0).doubleValue());
                    }
                    break;
                }
                case COT: {
                    SQLExpr argExpr = args0.get(0);
                    Object v0 = getValue(argExpr, args);
                    if (v0 != null) {
                        double d = Math.tan(NumValue.valueOf(v0).doubleValue());
                        if (d != 0.0) {
                            res = 1. / d;
                        }
                    }
                    break;
                }
                case CONCAT: {
                    List<Object> args1 = new ArrayList<>();
                    for (SQLExpr argExpr : args0) {
                        Object v0 = getValue(argExpr, args);
                        if (v0 == null) {
                            args1.clear(); // 如果有一个值为null则返回null
                            break;
                        }
                        args1.add(v0);
                    }
                    for (Object tmp : args1) {
                        if (res == null) {
                            res = "";
                        }
                        res = res.toString() + tmp;
                    }
                    break;
                }
                case CONCAT_WS: {
                    String split = null;
                    List<Object> args1 = new ArrayList<>();
                    int i = 0;
                    for (SQLExpr argExpr : args0) {
                        Object v0 = getValue(argExpr, args);
                        if (i++ == 0) {
                            split = v0 != null ? v0.toString() : null;
                            continue;
                        }
                        args1.add(v0);
                    }
                    if (split != null) {
                        i = 0;
                        for (Object tmp : args1) {
                            if (tmp == null) {
                                continue;
                            }
                            if (res == null) {
                                res = "";
                            }
                            if (i++ != 0) {
                                res = res.toString() + split;
                            }
                            res = res.toString() + tmp;
                        }
                    }
                    break;
                }
                case DEGREES: {
                    SQLExpr argExpr = args0.get(0);
                    Object v0 = getValue(argExpr, args);
                    if (v0 != null) {
                        res = Math.toDegrees(NumValue.valueOf(v0).doubleValue());
                    }
                    break;
                }
                case EXP: {
                    SQLExpr argExpr = args0.get(0);
                    Object v0 = getValue(argExpr, args);
                    if (v0 != null) {
                        res = Math.exp(NumValue.valueOf(v0).doubleValue());
                    }
                    break;
                }
                case FLOOR: {
                    SQLExpr argExpr = args0.get(0);
                    Object v0 = getValue(argExpr, args);
                    if (v0 != null) {
                        res = Math.floor(NumValue.valueOf(v0).doubleValue());
                    }
                    break;
                }
                case LOG: {
                    SQLExpr argExpr = args0.get(0);
                    Object v0 = getValue(argExpr, args);
                    if (v0 != null) {
                        res = Math.log(NumValue.valueOf(v0).doubleValue());
                    }
                    break;
                }
                case LOG10: {
                    SQLExpr argExpr = args0.get(0);
                    Object v0 = getValue(argExpr, args);
                    if (v0 != null) {
                        res = Math.log10(NumValue.valueOf(v0).doubleValue());
                    }
                    break;
                }
                case MOD: {
                    Object v0 = getValue(args0.get(0), args);
                    Object v1 = getValue(args0.get(1), args);
                    if (v0 != null && v1 != null) {
                        res = NumValue.valueOf(v0).mod(NumValue.valueOf(v1)).getValue();
                    }
                    break;
                }
                case PI: {
                    res = Math.PI;
                    break;
                }
                case POWER: {
                    Object v0 = getValue(args0.get(0), args);
                    Object v1 = getValue(args0.get(1), args);
                    if (v0 != null && v1 != null) {
                        res = Math.pow(NumValue.valueOf(v0).doubleValue(), NumValue.valueOf(v1).doubleValue());
                    }
                    break;
                }
                case RADIANS: {
                    SQLExpr argExpr = args0.get(0);
                    Object v0 = getValue(argExpr, args);
                    if (v0 != null) {
                        res = Math.toRadians(NumValue.valueOf(v0).doubleValue());
                    }
                    break;
                }
                case ROUND: {
                    Object v0 = getValue(args0.get(0), args);
                    Object v1 = args0.size() > 1 ? getValue(args0.get(1), args) : null;
                    double f;
                    if (v1 == null) {
                        f = 1.;
                    } else {
                        f = Math.pow(10., NumValue.valueOf(v1).doubleValue());
                    }
                    if (v0 != null) {
                        double middleResult = NumValue.valueOf(v0).doubleValue() * f;
                        int oneWithSymbol = middleResult > 0 ? 1 : -1;
                        res = Math.round(Math.abs(middleResult)) / f * oneWithSymbol;
                    }
                    break;
                }
                case SIGN: {
                    SQLExpr argExpr = args0.get(0);
                    Object v0 = getValue(argExpr, args);
                    if (v0 != null) {
                        res = NumValue.valueOf(v0).signum();
                    }
                    break;
                }
                case SIN: {
                    SQLExpr argExpr = args0.get(0);
                    Object v0 = getValue(argExpr, args);
                    if (v0 != null) {
                        res = Math.sin(NumValue.valueOf(v0).doubleValue());
                    }
                    break;
                }
                case SQRT: {
                    SQLExpr argExpr = args0.get(0);
                    Object v0 = getValue(argExpr, args);
                    if (v0 != null) {
                        res = Math.sqrt(NumValue.valueOf(v0).doubleValue());
                    }
                    break;
                }
                case TAN: {
                    SQLExpr argExpr = args0.get(0);
                    Object v0 = getValue(argExpr, args);
                    if (v0 != null) {
                        res = Math.tan(NumValue.valueOf(v0).doubleValue());
                    }
                    break;
                }
                case TRUNCATE: {
                    Object v0 = getValue(args0.get(0), args);
                    Object v1 = getValue(args0.get(1), args);
                    if (v0 != null) {
                        double d = NumValue.valueOf(v0).doubleValue();
                        NumValue n2 = null;
                        if (v1 != null) {
                            n2 = NumValue.valueOf(v1);
                        }
                        int p = n2 == null ? 0 : n2.intValue();
                        double f = Math.pow(10., p);
                        double g = d * f;
                        res = ((d < 0) ? Math.ceil(g) : Math.floor(g)) / f;
                    }
                    break;
                }
                case ASCII: {
                    SQLExpr argExpr = args0.get(0);
                    Object v0 = getValue(argExpr, args);
                    if (v0 != null) {
                        String s = v0.toString();
                        res = (int) s.charAt(0);
                    }
                    break;
                }
                case BIT_LENGTH: {
                    SQLExpr argExpr = args0.get(0);
                    Object v0 = getValue(argExpr, args);
                    if (v0 != null) {
                        res = 8 * length(v0);
                    }
                    break;
                }
                case CHAR: {
                    SQLExpr argExpr = args0.get(0);
                    Object v0 = getValue(argExpr, args);
                    if (v0 != null) {
                        res = (char) NumValue.valueOf(v0).intValue();
                    }
                    break;
                }
                case OCTET_LENGTH:
                case CHAR_LENGTH:
                case LENGTH: {
                    SQLExpr argExpr = args0.get(0);
                    Object v0 = getValue(argExpr, args);
                    if (v0 != null) {
                        res = length(v0);
                    }
                    break;
                }
                case INSTR: {
                    Object v0 = getValue(args0.get(0), args);
                    Object v1 = getValue(args0.get(1), args);
                    if (v0 != null && v1 != null) {
                        res = locate(v1.toString(), v0.toString(), 0);
                    }
                    break;
                }
                case LOWER:
                case LCASE: {
                    SQLExpr argExpr = args0.get(0);
                    Object v0 = getValue(argExpr, args);
                    if (v0 != null) {
                        res = v0.toString().toLowerCase();
                    }
                    break;
                }
                case LEFT: {
                    Object v0 = getValue(args0.get(0), args);
                    Object v1 = getValue(args0.get(1), args);
                    if (v0 != null && v1 != null) {
                        res = left(v0.toString(), NumValue.valueOf(v1).intValue());
                    }
                    break;
                }
                case LOCATE: {
                    Object v0 = getValue(args0.get(0), args);
                    Object v1 = getValue(args0.get(1), args);
                    Object v2 = args0.size() > 2 ? getValue(args0.get(2), args) : 0;
                    if (v0 != null && v1 != null) {
                        res = locate(v0.toString(), v1.toString(), NumValue.valueOf(v2).intValue());
                    }
                    break;
                }
                case LTRIM: {
                    Object v0 = getValue(args0.get(0), args);
                    if (v0 != null) {
                        res = StringUtils.trim(v0.toString(), true, false, " ");
                    }
                    break;
                }
                case REPEAT: {
                    Object v0 = getValue(args0.get(0), args);
                    Object v1 = getValue(args0.get(1), args);
                    if (v0 != null && v1 != null) {
                        int count = Math.max(0, NumValue.valueOf(v1).intValue());
                        res = repeat(v0.toString(), count);
                    }
                    break;
                }
                case REPLACE: {
                    Object v0 = getValue(args0.get(0), args);
                    Object v1 = getValue(args0.get(1), args);
                    Object v2 = args0.size() > 2 ? getValue(args0.get(2), args) : null;
                    if (v0 != null && v1 != null) {
                        String s1 = v0.toString();
                        String s2 = v1.toString();
                        String s3 = (v2 == null) ? "" : v2.toString();
                        res = StringUtils.replaceAll(s1, s2, s3);
                    }
                    break;
                }
                case RIGHT: {
                    Object v0 = getValue(args0.get(0), args);
                    Object v1 = getValue(args0.get(1), args);
                    if (v0 != null && v1 != null) {
                        res = right(v0.toString(), NumValue.valueOf(v1).intValue());
                    }
                    break;
                }
                case RTRIM: {
                    Object v0 = getValue(args0.get(0), args);
                    if (v0 != null) {
                        res = StringUtils.trim(v0.toString(), false, true, " ");
                    }
                    break;
                }
                case SPACE: {
                    SQLExpr argExpr = args0.get(0);
                    Object v0 = getValue(argExpr, args);
                    if (v0 != null) {
                        int len = Math.max(0, NumValue.valueOf(v0).intValue());
                        char[] chars = new char[len];
                        for (int i = len - 1; i >= 0; i--) {
                            chars[i] = ' ';
                        }
                        res = new String(chars);
                    }
                    break;
                }
                case SUBSTR:
                case SUBSTRING: {
                    Object v0 = getValue(args0.get(0), args);
                    Object v1 = getValue(args0.get(1), args);
                    if (v0 != null && v1 != null) {
                        String s = v0.toString();
                        int offset = NumValue.valueOf(v1).intValue();
                        if (offset < 0) {
                            offset = s.length() + offset + 1;
                        }
                        Object v2 = args0.size() > 2 ? getValue(args0.get(2), args) : null;
                        int length;
                        if (v2 == null) {
                            length = s.length();
                        } else {
                            length = NumValue.valueOf(v2).intValue();
                        }
                        res = substring(s, offset, length);
                    }
                    break;
                }
                case UPPER:
                case UCASE: {
                    SQLExpr argExpr = args0.get(0);
                    Object v0 = getValue(argExpr, args);
                    if (v0 != null) {
                        res = v0.toString().toUpperCase();
                    }
                    break;
                }
                case POSITION: {
                    Object v0 = getValue(args0.get(0), args);
                    Object v1 = getValue(args0.get(1), args);
                    if (v0 != null && v1 != null) {
                        res = locate(v0.toString(), v1.toString(), 0);
                    }
                    break;
                }
                case TRIM: {
                    Object v0 = getValue(args0.get(0), args);
                    if (v0 != null) {
                        res = StringUtils.trim(v0.toString(), true, true, " ");
                    }
                    break;
                }
                case RPAD: {
                    Object v0 = getValue(args0.get(0), args);
                    Object v1 = getValue(args0.get(1), args);
                    if (v0 != null && v1 != null) {
                        Object v2 = args0.size() > 2 ? getValue(args0.get(2), args) : null;
                        res = StringUtils.pad(v0.toString(),
                                NumValue.valueOf(v1).intValue(), v2 == null ? null : v2.toString(), true);
                    }
                    break;
                }
                case LPAD: {
                    Object v0 = getValue(args0.get(0), args);
                    Object v1 = getValue(args0.get(1), args);
                    if (v0 != null && v1 != null) {
                        Object v2 = args0.size() > 2 ? getValue(args0.get(2), args) : null;
                        res = StringUtils.pad(v0.toString(),
                                NumValue.valueOf(v1).intValue(), v2 == null ? null : v2.toString(), false);
                    }
                    break;
                }
                case TO_CHAR: {
                    Object v0 = getValue(args0.get(0), args);
                    if (v0 != null) {
                        res = v0.toString();
                    }
                    break;
                }
                case DATE_ADD: {
                    Date d1 = toDate(getValue(args0.get(0), args));
                    SQLExpr expr1 = args0.get(1);
                    if (d1 != null && expr1 instanceof SQLIntervalExpr) {
                        SQLIntervalExpr ie = (SQLIntervalExpr) expr1;
                        Number add = (Number) getValue(ie.getValue(), args);
                        if (ie.getUnit() == SQLIntervalUnit.YEAR) {
                            res = new DateTime(d1.getTime()).plusYears(add.intValue()).toDate();
                        } else if (ie.getUnit() == SQLIntervalUnit.MONTH || ie.getUnit() == SQLIntervalUnit.YEAR_MONTH) {
                            res = new DateTime(d1.getTime()).plusMonths(add.intValue()).toDate();
                        } else if (ie.getUnit() == SQLIntervalUnit.DAY) {
                            res = new DateTime(d1.getTime()).plusDays(add.intValue()).toDate();
                        } else if (ie.getUnit() == SQLIntervalUnit.HOUR || ie.getUnit() == SQLIntervalUnit.DAY_HOUR) {
                            res = new DateTime(d1.getTime()).plusHours(add.intValue()).toDate();
                        } else if (ie.getUnit() == SQLIntervalUnit.MINUTE || ie.getUnit() == SQLIntervalUnit.DAY_MINUTE ||
                                ie.getUnit() == SQLIntervalUnit.HOUR_MINUTE) {
                            res = new DateTime(d1.getTime()).plusMinutes(add.intValue()).toDate();
                        } else if (ie.getUnit() == SQLIntervalUnit.SECOND || ie.getUnit() == SQLIntervalUnit.DAY_SECOND ||
                                ie.getUnit() == SQLIntervalUnit.HOUR_SECOND || ie.getUnit() == SQLIntervalUnit.MINUTE_SECOND) {
                            res = new DateTime(d1.getTime()).plusSeconds(add.intValue()).toDate();
                        }
                    }
                    break;
                }
                case DATEDIFF: {
                    Date d1 = toDate(getValue(args0.get(0), args));
                    Date d2 = toDate(getValue(args0.get(1), args));
                    if (d1 != null && d2 != null) {
                        DateTime dt1 = new DateTime(d1.getTime()), dt2 = new DateTime(d2.getTime());
                        Period p = new Period(dt2, dt1, PeriodType.days());
                        res = p.getDays();
                    }
                    break;
                }
                case DAYNAME: {
                    Date d1 = toDate(getValue(args0.get(0), args));
                    if (d1 != null) {
                        int dow = new DateTime(d1.getTime()).getDayOfWeek();
                        res = WEEK_DAYS[dow - 1];
                    }
                    break;
                }
                case DAYOFMONTH: {
                    Date d1 = toDate(getValue(args0.get(0), args));
                    if (d1 != null) {
                        res = new DateTime(d1.getTime()).dayOfMonth().get();
                    }
                    break;
                }
                case DAYOFWEEK: {
                    Date d1 = toDate(getValue(args0.get(0), args));
                    if (d1 != null) {
                        int dw = new DateTime(d1.getTime()).dayOfWeek().get();
                        if (dw == 7) {
                            res = 1;
                        } else {
                            res = dw + 1;
                        }
                    }
                    break;
                }
                case DAYOFYEAR: {
                    Date d1 = toDate(getValue(args0.get(0), args));
                    if (d1 != null) {
                        res = new DateTime(d1.getTime()).dayOfYear().get();
                    }
                    break;
                }
                case HOUR: {
                    Date d1 = toDate(getValue(args0.get(0), args));
                    if (d1 != null) {
                        res = new DateTime(d1.getTime()).hourOfDay().get();
                    }
                    break;
                }
                case MINUTE: {
                    Date d1 = toDate(getValue(args0.get(0), args));
                    if (d1 != null) {
                        res = new DateTime(d1.getTime()).minuteOfHour().get();
                    }
                    break;
                }
                case MONTH: {
                    Date d1 = toDate(getValue(args0.get(0), args));
                    if (d1 != null) {
                        res = new DateTime(d1.getTime()).monthOfYear().get();
                    }
                    break;
                }
                case MONTHNAME: {
                    Date d1 = toDate(getValue(args0.get(0), args));
                    if (d1 != null) {
                        int month = new DateTime(d1.getTime()).monthOfYear().get();
                        res = MONTH_NAMES[month - 1];
                    }
                    break;
                }
                case QUARTER: {
                    Date d1 = toDate(getValue(args0.get(0), args));
                    if (d1 != null) {
                        int month = new DateTime(d1.getTime()).monthOfYear().get();
                        if (month >= 1 && month <= 3) {
                            res = 1;
                        } else if (month >= 4 && month <= 6) {
                            res = 2;
                        } else if (month >= 7 && month <= 9) {
                            res = 3;
                        } else {
                            res = 4;
                        }
                    }
                    break;
                }
                case SECOND: {
                    Date d1 = toDate(getValue(args0.get(0), args));
                    if (d1 != null) {
                        res = new DateTime(d1.getTime()).secondOfMinute().get();
                    }
                    break;
                }
                case WEEK: {
                    Date d1 = toDate(getValue(args0.get(0), args));
                    if (d1 != null) {
                        res = new DateTime(d1.getTime()).weekOfWeekyear().get();
                    }
                    break;
                }
                case YEAR: {
                    Date d1 = toDate(getValue(args0.get(0), args));
                    if (d1 != null) {
                        res = new DateTime(d1.getTime()).year().get();
                    }
                    break;
                }
                case IFNULL: {
                    Object v0 = getValue(args0.get(0), args);
                    Object v1 = getValue(args0.get(1), args);
                    if (v0 == null) {
                        res = v1;
                    } else {
                        res = v0;
                    }
                    break;
                }
                case NULLIF: {
                    Object v0 = getValue(args0.get(0), args);
                    Object v1 = getValue(args0.get(1), args);
                    if (v0 != null && v1 != null) {
                        if (v0 instanceof Timestamp) {
                            v0 = new DateTime(((Timestamp) v0).getTime()).toString("yyyy-MM-dd HH:mm:ss");
                        }
                        if (v0 instanceof java.sql.Date) {
                            v0 = new DateTime(((Date) v0).getTime()).toString("yyyy-MM-dd");
                        }
                        if (v0 instanceof java.sql.Time) {
                            v0 = new DateTime(((Date) v0).getTime()).toString("HH:mm:ss");
                        }
                        if (!v0.toString().equals(v1.toString())) {
                            res = v0;
                        }
                    } else if (v0 != null) {
                        res = v0;
                    }
                    break;
                }
                case ISNULL: {
                    Object v0 = getValue(args0.get(0), args);
                    if (v0 == null) {
                        res = (byte) 1;
                    } else {
                        res = (byte) 0;
                    }
                    break;
                }
                default:
                    break;
            }
        } else if (expr instanceof SQLIdentifierExpr) {
            // 无owner
            SQLIdentifierExpr ie = (SQLIdentifierExpr) expr;
            res = getArgValue(args, Util.cleanColumn(ie.getName()));
        } else if (expr instanceof SQLPropertyExpr) {
            // 有owner
            SQLPropertyExpr pe = (SQLPropertyExpr) expr;
            res = getArgValue(args, pe.getOwner() + "." + Util.cleanColumn(pe.getName()));
        }
        return res;
    }

    private static Integer length(Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof byte[]) {
            return ((byte[]) v).length;
        } else {
            return v.toString().length();
        }
    }

    private static Object getArgValue(Map<String, Object> values, String column) {
        if (!values.containsKey(column)) {
            throw new RuntimeException("Value of column: " + column + " not found!");
        }
        return values.get(column);
    }

    private static Integer compareValues(Object v0, Object v1) {
        if (v0 == null || v1 == null) {
            return null;
        }

        //如果有一个是数字则按数字来比较
        if (v0 instanceof Number || v1 instanceof Number) {
            NumValue n1 = NumValue.valueOf(v0);
            NumValue n2 = NumValue.valueOf(v1);
            return n1.compareTo(n2);
        } else {
            return v0.toString().compareTo(v1.toString());
        }
    }

    private static String left(String s, int count) {
        if (count < 0) {
            count = 0;
        } else if (count > s.length()) {
            count = s.length();
        }
        return s.substring(0, count);
    }

    private static int locate(String search, String s, int start) {
        if (start < 0) {
            int i = s.length() + start;
            return s.lastIndexOf(search, i) + 1;
        }
        int i = (start == 0) ? 0 : start - 1;
        return s.indexOf(search, i) + 1;
    }

    private static String repeat(String s, int count) {
        StringBuilder buff = new StringBuilder(s.length() * count);
        while (count-- > 0) {
            buff.append(s);
        }
        return buff.toString();
    }

    private static String right(String s, int count) {
        if (count < 0) {
            count = 0;
        } else if (count > s.length()) {
            count = s.length();
        }
        return s.substring(s.length() - count);
    }

    private static String substring(String s, int start, int length) {
        int len = s.length();
        start--;
        if (start < 0) {
            start = 0;
        }
        if (length < 0) {
            length = 0;
        }
        start = (start > len) ? len : start;
        if (start + length > len) {
            length = len - start;
        }
        return s.substring(start, start + length);
    }

    private static Date toDate(Object v) {
        Date date = null;
        if (v != null) {
            if (v instanceof String) {
                date = Util.parseDate((String) v);
            } else if (v instanceof Date) {
                date = (Date) v;
            }
        }
        return date;
    }
}
