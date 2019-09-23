package com.alibaba.otter.canal.adapter.es.core.parser.sql;

import static com.alibaba.fastsql.sql.ast.expr.SQLBinaryOperator.BooleanAnd;
import static com.alibaba.fastsql.sql.ast.expr.SQLBinaryOperator.BooleanOr;

import java.util.Date;
import java.util.Map;

import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.otter.canal.adapter.es.core.util.Util;

public class Condition {
    public static boolean compareValue(SQLExpr whereExpr, Map<String, Object> args) {
        boolean res = true;
        if (!(whereExpr instanceof SQLBinaryOpExpr)) {
            return false;
        }
        SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) whereExpr;
        if (sqlBinaryOpExpr.getOperator() == BooleanAnd) {
            res = compareValue(sqlBinaryOpExpr.getLeft(), args);
            res &= compareValue(sqlBinaryOpExpr.getRight(), args);
        } else if (sqlBinaryOpExpr.getOperator() == BooleanOr) {
            res = compareValue(sqlBinaryOpExpr.getLeft(), args);
            res |= compareValue(sqlBinaryOpExpr.getRight(), args);
        } else {
            switch (sqlBinaryOpExpr.getOperator()) {
                case Equality: {
                    Object leftVal = Function.getValue(sqlBinaryOpExpr.getLeft(), args);
                    Object rightVal = Function.getValue(sqlBinaryOpExpr.getRight(), args);
                    if (leftVal == null || rightVal == null) {
                        res = false;
                    } else if (leftVal instanceof Date || rightVal instanceof Date) {
                        Date ldt = obj2Date(leftVal);
                        Date rdt = obj2Date(rightVal);
                        if (ldt == null || rdt == null || ldt.getTime() != rdt.getTime()) {
                            res = false;
                        }
                    } else {
                        if (!leftVal.toString().equals(rightVal.toString())) {
                            res = false;
                        }
                    }
                    break;
                }
                case NotEqual:
                case LessThanOrGreater: {
                    Object leftVal = Function.getValue(sqlBinaryOpExpr.getLeft(), args);
                    Object rightVal = Function.getValue(sqlBinaryOpExpr.getRight(), args);
                    if (leftVal == null || rightVal == null) {
                        res = false;
                    } else if (leftVal instanceof Date || rightVal instanceof Date) {
                        Date ldt = obj2Date(leftVal);
                        Date rdt = obj2Date(rightVal);
                        if (ldt == null || rdt == null || ldt.getTime() == rdt.getTime()) {
                            res = false;
                        }
                    } else {
                        if (leftVal.toString().equals(rightVal.toString())) {
                            res = false;
                        }
                    }
                    break;
                }
                case GreaterThan: {
                    Object leftVal = Function.getValue(sqlBinaryOpExpr.getLeft(), args);
                    Object rightVal = Function.getValue(sqlBinaryOpExpr.getRight(), args);
                    if (leftVal == null || rightVal == null) {
                        res = false;
                    } else if (leftVal instanceof Number || rightVal instanceof Number) {
                        NumValue lval = NumValue.valueOf(leftVal);
                        NumValue rval = NumValue.valueOf(rightVal);
                        res = lval.compareTo(rval) > 0;
                    } else if (leftVal instanceof Date || rightVal instanceof Date) {
                        Date ldt = obj2Date(leftVal);
                        Date rdt = obj2Date(rightVal);
                        if (ldt == null || rdt == null || ldt.getTime() <= rdt.getTime()) {
                            res = false;
                        }
                    } else {
                        res = leftVal.toString().compareTo(rightVal.toString()) > 0;
                    }
                    break;
                }
                case NotLessThan:
                case GreaterThanOrEqual: {
                    Object leftVal = Function.getValue(sqlBinaryOpExpr.getLeft(), args);
                    Object rightVal = Function.getValue(sqlBinaryOpExpr.getRight(), args);
                    if (leftVal == null || rightVal == null) {
                        res = false;
                    } else if (leftVal instanceof Number || rightVal instanceof Number) {
                        NumValue lval = NumValue.valueOf(leftVal);
                        NumValue rval = NumValue.valueOf(rightVal);
                        res = lval.compareTo(rval) >= 0;
                    } else if (leftVal instanceof Date || rightVal instanceof Date) {
                        Date ldt = obj2Date(leftVal);
                        Date rdt = obj2Date(rightVal);
                        if (ldt == null || rdt == null || ldt.getTime() < rdt.getTime()) {
                            res = false;
                        }
                    } else {
                        res = leftVal.toString().compareTo(rightVal.toString()) >= 0;
                    }
                    break;
                }
                case LessThan: {
                    Object leftVal = Function.getValue(sqlBinaryOpExpr.getLeft(), args);
                    Object rightVal = Function.getValue(sqlBinaryOpExpr.getRight(), args);
                    if (leftVal == null || rightVal == null) {
                        res = false;
                    } else if (leftVal instanceof Number || rightVal instanceof Number) {
                        NumValue lval = NumValue.valueOf(leftVal);
                        NumValue rval = NumValue.valueOf(rightVal);
                        res = lval.compareTo(rval) < 0;
                    } else if (leftVal instanceof Date || rightVal instanceof Date) {
                        Date ldt = obj2Date(leftVal);
                        Date rdt = obj2Date(rightVal);
                        if (ldt == null || rdt == null || ldt.getTime() >= rdt.getTime()) {
                            res = false;
                        }
                    } else {
                        res = leftVal.toString().compareTo(rightVal.toString()) < 0;
                    }
                    break;
                }
                case NotGreaterThan:
                case LessThanOrEqual: {
                    Object leftVal = Function.getValue(sqlBinaryOpExpr.getLeft(), args);
                    Object rightVal = Function.getValue(sqlBinaryOpExpr.getRight(), args);
                    if (leftVal == null || rightVal == null) {
                        res = false;
                    } else if (leftVal instanceof Number || rightVal instanceof Number) {
                        NumValue lval = NumValue.valueOf(leftVal);
                        NumValue rval = NumValue.valueOf(rightVal);
                        res = lval.compareTo(rval) <= 0;
                    } else if (leftVal instanceof Date || rightVal instanceof Date) {
                        Date ldt = obj2Date(leftVal);
                        Date rdt = obj2Date(rightVal);
                        if (ldt == null || rdt == null || ldt.getTime() > rdt.getTime()) {
                            res = false;
                        }
                    } else {
                        res = leftVal.toString().compareTo(rightVal.toString()) <= 0;
                    }
                    break;
                }
                case Is:
                case IsNot: {
                    Object leftVal = Function.getValue(sqlBinaryOpExpr.getLeft(), args);
                    Object rightVal = Function.getValue(sqlBinaryOpExpr.getRight(), args);
                    if (rightVal != null) {
                        throw new UnsupportedOperationException();
                    }
                    if (sqlBinaryOpExpr.getOperator() == SQLBinaryOperator.Is) {
                        res = leftVal == null;
                    } else {
                        res = leftVal != null;
                    }
                    break;
                }
                /*case ILike:
                case Like:
                case NotILike:
                case NotLike: {
                    boolean ignoreCase = false;
                    if (sqlBinaryOpExpr.getOperator() == ILike || sqlBinaryOpExpr.getOperator() == NotILike) {
                        ignoreCase = true;
                    }
                    Object leftVal = Function.getValue(sqlBinaryOpExpr.getLeft(), args);
                    Object rightVal = Function.getValue(sqlBinaryOpExpr.getRight(), args);
                    if (!(leftVal instanceof String) || !(rightVal instanceof String)) {
                        throw new UnsupportedOperationException();
                    }
                    String lstr = (String) leftVal;
                    String rstr = (String) rightVal;
                    if (rstr.startsWith("%") && rstr.endsWith("%")) {
                        String p = rstr.substring(1, rstr.length() - 1);
                        if (ignoreCase) {
                            res = containsIgnoreCase(lstr, p);
                        } else {
                            res = lstr.contains(p);
                        }
                    } else if (rstr.startsWith("%")) {
                        res = lstr.regionMatches(ignoreCase, lstr.length() - rstr.length() + 1, rstr, 1, rstr.length() - 1);
                    } else if (rstr.endsWith("%")) {
                        res = lstr.regionMatches(ignoreCase, 0, rstr, 0, rstr.length() - 1);
                    }

                    if (sqlBinaryOpExpr.getOperator() == NotLike || sqlBinaryOpExpr.getOperator() == NotILike) {
                        res = !res;
                    }
                    break;
                }*/
                default:
                    throw new UnsupportedOperationException();
            }
        }
        return res;
    }

    private static Date obj2Date(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Date) {
            return (Date) obj;
        } else {
            return Util.parseDate(obj.toString());
        }
    }

    /*private static boolean containsIgnoreCase(String src, String what) {
        final int length = what.length();
        if (length == 0) {
            // Empty string is contained
            return true;
        }

        final char firstLo = Character.toLowerCase(what.charAt(0));
        final char firstUp = Character.toUpperCase(what.charAt(0));

        for (int i = src.length() - length; i >= 0; i--) {
            // Quick check before calling the more expensive regionMatches()
            final char ch = src.charAt(i);
            if (ch != firstLo && ch != firstUp) {
                continue;
            }
            if (src.regionMatches(true, i, what, 0, length)) {
                return true;
            }
        }

        return false;
    }*/
}
