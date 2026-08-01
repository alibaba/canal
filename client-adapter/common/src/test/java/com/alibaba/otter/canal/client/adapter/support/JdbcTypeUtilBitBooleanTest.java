package com.alibaba.otter.canal.client.adapter.support;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for JdbcTypeUtil.jdbcType2javaType() and JdbcTypeUtil.getRSData()
 * specifically targeting the BIT and BOOLEAN type mapping.
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

    /**
     * Types.BIT should be read from the ResultSet as a Boolean.
     */
    @Test
    public void getRSDataInputBITOutputBoolean() throws SQLException {
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.getBoolean("c1")).thenReturn(true);

        Object actual = JdbcTypeUtil.getRSData(rs, "c1", Types.BIT);
        Assert.assertEquals("Types.BIT should be read as a Boolean", Boolean.TRUE, actual);
    }

    /**
     * Types.BOOLEAN should be read from the ResultSet as a Boolean.
     */
    @Test
    public void getRSDataInputBOOLEANOutputBoolean() throws SQLException {
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.getBoolean("c1")).thenReturn(false);

        Object actual = JdbcTypeUtil.getRSData(rs, "c1", Types.BOOLEAN);
        Assert.assertEquals("Types.BOOLEAN should be read as a Boolean", Boolean.FALSE, actual);
    }

    /**
     * getRSData and jdbcType2javaType are used together by HbaseEtlService:
     * the value returned by the former is written with the type reported by the
     * latter (Type.getType / PhType.getType). If they disagree the NATIVE and
     * PHOENIX modes fail with a ClassCastException, so keep them in sync.
     */
    @Test
    public void getRSDataIsInstanceOfJdbcType2javaTypeForBIT() throws SQLException {
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.getBoolean("c1")).thenReturn(true);

        Object actual = JdbcTypeUtil.getRSData(rs, "c1", Types.BIT);
        Assert.assertTrue("getRSData must return an instance of jdbcType2javaType(Types.BIT)",
            JdbcTypeUtil.jdbcType2javaType(Types.BIT).isInstance(actual));
    }
}
