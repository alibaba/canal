package com.alibaba.otter.canal.client.adapter.support;

import java.sql.Types;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for JdbcTypeUtil.jdbcType2javaType() specifically targeting
 * the BIT and BOOLEAN type mapping.
 */
public class JdbcTypeUtilBitBooleanTest {

    /**
     * Types.BIT should map to Boolean.class, not Byte.TYPE.
     * The switch case has a commented-out return and no break,
     * causing fall-through to Types.TINYINT which returns Byte.TYPE.
     */
    @Test
    public void jdbcType2javaTypeInputBITOutputBooleanClass() {
        Class<?> actual = JdbcTypeUtil.jdbcType2javaType(Types.BIT);
        Assert.assertEquals("Types.BIT should map to Boolean.class", Boolean.class, actual);
    }

    /**
     * Types.BOOLEAN should map to Boolean.class, not Byte.TYPE.
     * Same fall-through bug as Types.BIT.
     */
    @Test
    public void jdbcType2javaTypeInputBOOLEANOutputBooleanClass() {
        Class<?> actual = JdbcTypeUtil.jdbcType2javaType(Types.BOOLEAN);
        Assert.assertEquals("Types.BOOLEAN should map to Boolean.class", Boolean.class, actual);
    }

    /**
     * Types.TINYINT should still correctly map to Byte.TYPE.
     * Ensures the fix doesn't break the TINYINT case.
     */
    @Test
    public void jdbcType2javaTypeInputTINYINTOutputByteType() {
        Class<?> actual = JdbcTypeUtil.jdbcType2javaType(Types.TINYINT);
        Assert.assertEquals("Types.TINYINT should map to Byte.TYPE", Byte.TYPE, actual);
    }
}
