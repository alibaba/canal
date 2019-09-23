package com.alibaba.otter.canal.adapter.es.core.parser.sql;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;

public class NumValue extends Number {

    private static final long serialVersionUID = 7139015450568346916L;

    private final static int INTEGER = 1, DECIMAL = 2;

    private Number value;
    private int type;

    public NumValue(Number value, int type) {
        this.value = value;
        this.type = type;
    }

    public static NumValue valueOf(Object value) {
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long
                || value instanceof BigInteger) {
            return integerValue((Number) value);
        } else if (value instanceof Float || value instanceof Double || value instanceof BigDecimal) {
            return decimalValue((Number) value);
        } else {
            return str2Num(value.toString());
        }
    }

    private static NumValue str2Num(String value) {
        if (value == null) {
            return null;
        }
        int len = value.length();
        StringBuilder sb = new StringBuilder();
        boolean point = false;
        for (int i = 0; i < len; i++) {
            if (i == 0 && value.charAt(i) == '-') {
                sb.append("-");
                continue;
            }
            char c = value.charAt(i);
            if ((int) c >= (int) '0' && (int) c <= (int) '9') {
                sb.append(c);
            } else if ((int) c == (int) '.' && !point) {
                sb.append(c);
                point = true;
            } else {
                break;
            }
        }
        String resNumStr = sb.toString();
        if (resNumStr.endsWith(".")) {
            resNumStr = resNumStr.substring(0, resNumStr.length() - 1);
        }
        if (resNumStr.length() == 0 || resNumStr.equals("-")) {
            return integerValue(0);
        }
        if (resNumStr.contains(".")) {
            return decimalValue(new BigDecimal(resNumStr));
        } else {
            return integerValue(new BigInteger(resNumStr));
        }
    }

    private static NumValue integerValue(Number value) {
        BigInteger val;
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
            val = BigInteger.valueOf((value).longValue());
        } else if (value instanceof BigInteger) {
            val = (BigInteger) value;
        } else {
            throw new IllegalArgumentException();
        }
        return new NumValue(val, INTEGER);
    }

    private static NumValue decimalValue(Number value) {
        BigDecimal val;
        if (value instanceof Float || value instanceof Double) {
            val = BigDecimal.valueOf(value.doubleValue());
        } else if (value instanceof BigDecimal) {
            val = (BigDecimal) value;
        } else {
            throw new IllegalArgumentException();
        }
        return new NumValue(val, DECIMAL);
    }

    public Number getValue() {
        return value;
    }

    public void setValue(Number value) {
        this.value = value;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    @Override
    public int intValue() {
        return value.intValue();
    }

    @Override
    public long longValue() {
        return value.longValue();
    }

    @Override
    public float floatValue() {
        return value.floatValue();
    }

    @Override
    public double doubleValue() {
        return value.doubleValue();
    }

    public NumValue nextProbablePrime() {
        if (this.type != INTEGER) {
            return null;
        }
        BigInteger res = ((BigInteger) value).nextProbablePrime();
        return integerValue(res);
    }

    public NumValue add(NumValue val) {
        if (this.type == DECIMAL && val.type == DECIMAL) {
            BigDecimal res = ((BigDecimal) this.value).add((BigDecimal) val.value);
            return decimalValue(res);
        } else if (this.type == DECIMAL) {
            BigDecimal res = ((BigDecimal) this.value).add(new BigDecimal((val.value).toString()));
            return decimalValue(res);
        } else if (val.type == DECIMAL) {
            BigDecimal res = new BigDecimal(this.value.toString()).add((BigDecimal) val.value);
            return decimalValue(res);
        } else {
            BigInteger res = ((BigInteger) this.value).add((BigInteger) val.value);
            return integerValue(res);
        }
    }

    public NumValue subtract(NumValue val) {
        if (this.type == DECIMAL && val.type == DECIMAL) {
            BigDecimal res = ((BigDecimal) this.value).subtract((BigDecimal) val.value);
            return decimalValue(res);
        } else if (this.type == DECIMAL) {
            BigDecimal res = ((BigDecimal) this.value).subtract(new BigDecimal((val.value).toString()));
            return decimalValue(res);
        } else if (val.type == DECIMAL) {
            BigDecimal res = new BigDecimal(this.value.toString()).subtract((BigDecimal) val.value);
            return decimalValue(res);
        } else {
            BigInteger res = ((BigInteger) this.value).subtract((BigInteger) val.value);
            return integerValue(res);
        }
    }

    public NumValue multiply(NumValue val) {
        if (this.type == DECIMAL && val.type == DECIMAL) {
            BigDecimal res = ((BigDecimal) this.value).multiply((BigDecimal) val.value);
            return decimalValue(res);
        } else if (this.type == DECIMAL) {
            BigDecimal res = ((BigDecimal) this.value).multiply(new BigDecimal((val.value).toString()));
            return decimalValue(res);
        } else if (val.type == DECIMAL) {
            BigDecimal res = new BigDecimal(this.value.toString()).multiply((BigDecimal) val.value);
            return decimalValue(res);
        } else {
            BigInteger res = ((BigInteger) this.value).multiply((BigInteger) val.value);
            return integerValue(res);
        }
    }

    public NumValue divide(NumValue val) {
        if (this.type == DECIMAL && val.type == DECIMAL) {
            BigDecimal res = ((BigDecimal) this.value).divide((BigDecimal) val.value, MathContext.DECIMAL32);
            return decimalValue(res);
        } else if (this.type == DECIMAL) {
            BigDecimal res = ((BigDecimal) this.value).divide(new BigDecimal((val.value).toString()),
                    MathContext.DECIMAL32);
            return decimalValue(res);
        } else if (val.type == DECIMAL) {
            BigDecimal res = new BigDecimal(this.value.toString()).divide((BigDecimal) val.value,
                    MathContext.DECIMAL32);
            return decimalValue(res);
        } else {
            BigInteger res = ((BigInteger) this.value).divide((BigInteger) val.value);
            return integerValue(res);
        }
    }

    public NumValue[] divideAndRemainder(NumValue val) {
        BigDecimal[] resBD = null;
        BigInteger[] resBI = null;
        if (this.type == DECIMAL && val.type == DECIMAL) {
            resBD = ((BigDecimal) this.value).divideAndRemainder((BigDecimal) val.value, MathContext.DECIMAL32);
        } else if (this.type == DECIMAL) {
            resBD = ((BigDecimal) this.value).divideAndRemainder(new BigDecimal((val.value).toString()),
                    MathContext.DECIMAL32);
        } else if (val.type == DECIMAL) {
            resBD = new BigDecimal(this.value.toString()).divideAndRemainder((BigDecimal) val.value,
                    MathContext.DECIMAL32);
        } else {
            resBI = ((BigInteger) this.value).divideAndRemainder((BigInteger) val.value);
        }

        if (resBD != null) {
            NumValue[] res = new NumValue[resBD.length];
            for (int i = 0; i < resBD.length; i++) {
                res[i] = decimalValue(res[i]);
            }
            return res;
        }
        if (resBI != null) {
            NumValue[] res = new NumValue[resBI.length];
            for (int i = 0; i < resBI.length; i++) {
                res[i] = integerValue(res[i]);
            }
            return res;
        }
        return null;
    }

    public NumValue remainder(NumValue val) {
        if (this.type == DECIMAL && val.type == DECIMAL) {
            BigDecimal res = ((BigDecimal) this.value).remainder((BigDecimal) val.value);
            return decimalValue(res);
        } else if (this.type == DECIMAL) {
            BigDecimal res = ((BigDecimal) this.value).remainder(new BigDecimal((val.value).toString()));
            return decimalValue(res);
        } else if (val.type == DECIMAL) {
            BigDecimal res = new BigDecimal(this.value.toString()).remainder((BigDecimal) val.value);
            return decimalValue(res);
        } else {
            BigInteger res = ((BigInteger) this.value).remainder((BigInteger) val.value);
            return integerValue(res);
        }
    }

    public NumValue pow(int exponent) {
        if (this.type == DECIMAL) {
            BigDecimal res = ((BigDecimal) this.value).pow(exponent);
            return decimalValue(res);
        } else {
            BigInteger res = ((BigInteger) this.value).pow(exponent);
            return integerValue(res);
        }
    }

    public NumValue abs() {
        if (this.type == DECIMAL) {
            BigDecimal res = ((BigDecimal) this.value).abs();
            return decimalValue(res);
        } else {
            BigInteger res = ((BigInteger) this.value).abs();
            return integerValue(res);
        }
    }

    public NumValue negate() {
        if (this.type == DECIMAL) {
            BigDecimal res = ((BigDecimal) this.value).negate();
            return decimalValue(res);
        } else {
            BigInteger res = ((BigInteger) this.value).negate();
            return integerValue(res);
        }
    }

    public int signum() {
        if (this.type == DECIMAL) {
            return ((BigDecimal) this.value).signum();
        } else {
            return ((BigInteger) this.value).signum();
        }
    }

    public NumValue mod(NumValue val) {
        if (this.type == DECIMAL && val.type == DECIMAL) {
            BigDecimal res = ((BigDecimal) this.value).divideAndRemainder((BigDecimal) val.value)[1];
            return decimalValue(res);
        } else if (this.type == DECIMAL) {
            BigDecimal res = ((BigDecimal) this.value).divideAndRemainder(new BigDecimal((val.value).toString()))[1];
            return decimalValue(res);
        } else if (val.type == DECIMAL) {
            BigDecimal res = new BigDecimal(this.value.toString()).divideAndRemainder((BigDecimal) val.value)[1];
            return decimalValue(res);
        } else {
            BigInteger res = ((BigInteger) this.value).mod((BigInteger) val.value);
            return integerValue(res);
        }
    }

    public NumValue shiftLeft(int n) {
        if (this.type == INTEGER) {
            BigInteger res = ((BigInteger) this.value).shiftLeft(n);
            return integerValue(res);
        } else {
            BigInteger res = ((BigDecimal) this.value).toBigInteger().shiftLeft(n);
            return integerValue(res);
        }
    }

    public NumValue shiftRight(int n) {
        if (this.type == INTEGER) {
            BigInteger res = ((BigInteger) this.value).shiftRight(n);
            return integerValue(res);
        } else {
            BigInteger res = ((BigDecimal) this.value).toBigInteger().shiftRight(n);
            return integerValue(res);
        }
    }

    public NumValue and(NumValue val) {
        if (this.type == DECIMAL && val.type == DECIMAL) {
            BigInteger res = ((BigDecimal) this.value).toBigInteger().add(((BigDecimal) val.value).toBigInteger());
            return integerValue(res);
        } else if (this.type == DECIMAL) {
            BigInteger res = ((BigDecimal) this.value).toBigInteger().add(((BigInteger) val.value));
            return integerValue(res);
        } else if (val.type == DECIMAL) {
            BigInteger res = ((BigInteger) this.value).add(((BigDecimal) val.value).toBigInteger());
            return integerValue(res);
        } else {
            BigInteger res = ((BigInteger) this.value).add((BigInteger) val.value);
            return integerValue(res);
        }
    }

    public NumValue or(NumValue val) {
        if (this.type == DECIMAL && val.type == DECIMAL) {
            BigInteger res = ((BigDecimal) this.value).toBigInteger().or(((BigDecimal) val.value).toBigInteger());
            return integerValue(res);
        } else if (this.type == DECIMAL) {
            BigInteger res = ((BigDecimal) this.value).toBigInteger().or(((BigInteger) val.value));
            return integerValue(res);
        } else if (val.type == DECIMAL) {
            BigInteger res = ((BigInteger) this.value).or(((BigDecimal) val.value).toBigInteger());
            return integerValue(res);
        } else {
            BigInteger res = ((BigInteger) this.value).or((BigInteger) val.value);
            return integerValue(res);
        }
    }

    public NumValue xor(NumValue val) {
        if (this.type == DECIMAL && val.type == DECIMAL) {
            BigInteger res = ((BigDecimal) this.value).toBigInteger().xor(((BigDecimal) val.value).toBigInteger());
            return integerValue(res);
        } else if (this.type == DECIMAL) {
            BigInteger res = ((BigDecimal) this.value).toBigInteger().xor(((BigInteger) val.value));
            return integerValue(res);
        } else if (val.type == DECIMAL) {
            BigInteger res = ((BigInteger) this.value).xor(((BigDecimal) val.value).toBigInteger());
            return integerValue(res);
        } else {
            BigInteger res = ((BigInteger) this.value).xor((BigInteger) val.value);
            return integerValue(res);
        }
    }

    public NumValue not() {
        if (this.type == DECIMAL) {
            BigInteger res = ((BigDecimal) this.value).toBigInteger().not();
            return integerValue(res);
        } else {
            BigInteger res = ((BigInteger) this.value).not();
            return integerValue(res);
        }
    }

    public NumValue andNot(NumValue val) {
        if (this.type == DECIMAL && val.type == DECIMAL) {
            BigInteger res = ((BigDecimal) this.value).toBigInteger().andNot(((BigDecimal) val.value).toBigInteger());
            return integerValue(res);
        } else if (this.type == DECIMAL) {
            BigInteger res = ((BigDecimal) this.value).toBigInteger().andNot(((BigInteger) val.value));
            return integerValue(res);
        } else if (val.type == DECIMAL) {
            BigInteger res = ((BigInteger) this.value).andNot(((BigDecimal) val.value).toBigInteger());
            return integerValue(res);
        } else {
            BigInteger res = ((BigInteger) this.value).andNot((BigInteger) val.value);
            return integerValue(res);
        }
    }

    public NumValue min(NumValue val) {
        if (this.type == DECIMAL && val.type == DECIMAL) {
            BigDecimal res = ((BigDecimal) this.value).min((BigDecimal) val.value);
            return decimalValue(res);
        } else if (this.type == DECIMAL) {
            BigDecimal res = ((BigDecimal) this.value).min(new BigDecimal((val.value).toString()));
            return decimalValue(res);
        } else if (val.type == DECIMAL) {
            BigDecimal res = new BigDecimal(this.value.toString()).min((BigDecimal) val.value);
            return decimalValue(res);
        } else {
            BigInteger res = ((BigInteger) this.value).min((BigInteger) val.value);
            return integerValue(res);
        }
    }

    public NumValue max(NumValue val) {
        if (this.type == DECIMAL && val.type == DECIMAL) {
            BigDecimal res = ((BigDecimal) this.value).max((BigDecimal) val.value);
            return decimalValue(res);
        } else if (this.type == DECIMAL) {
            BigDecimal res = ((BigDecimal) this.value).max(new BigDecimal((val.value).toString()));
            return decimalValue(res);
        } else if (val.type == DECIMAL) {
            BigDecimal res = new BigDecimal(this.value.toString()).max((BigDecimal) val.value);
            return decimalValue(res);
        } else {
            BigInteger res = ((BigInteger) this.value).max((BigInteger) val.value);
            return integerValue(res);
        }
    }

    public int compareTo(NumValue val) {
        if (this.type == DECIMAL && val.type == DECIMAL) {
            return ((BigDecimal) this.value).compareTo((BigDecimal) val.value);
        } else if (this.type == DECIMAL) {
            return ((BigDecimal) this.value).compareTo(new BigDecimal((val.value).toString()));
        } else if (val.type == DECIMAL) {
            return new BigDecimal(this.value.toString()).compareTo((BigDecimal) val.value);
        } else {
            return ((BigInteger) this.value).compareTo((BigInteger) val.value);
        }
    }

    public NumValue toDecimal() {
        if (this.type == INTEGER) {
            this.value = new BigDecimal(this.value.toString());
            this.type = DECIMAL;
        }
        return this;
    }

    public static void main(String[] args) {
        BigDecimal b = new BigDecimal("24235212142353.3453234234");
        System.out.println(b.toBigInteger());
    }
}
