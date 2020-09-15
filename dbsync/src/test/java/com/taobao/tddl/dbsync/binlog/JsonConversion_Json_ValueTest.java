package com.taobao.tddl.dbsync.binlog;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

import com.taobao.tddl.dbsync.binlog.JsonConversion.Json_Value;
import com.taobao.tddl.dbsync.binlog.JsonConversion.Json_enum_type;

public class JsonConversion_Json_ValueTest {

    @Rule
    public final ExpectedException thrown        = ExpectedException.none();

    @SuppressWarnings("deprecation")
    @Rule
    public final Timeout           globalTimeout = new Timeout(10000);

    /* testedClasses: JsonConversion_Json_Value */
    // Test written by Diffblue Cover.

    @Test
    public void constructorInputNullNotNullOutputVoid() {

        // Arrange
        final Json_enum_type t = null;
        final String value = ",";

        // Act, creating object to test constructor
        final Json_Value objectUnderTest = new Json_Value(t, value);

        // Assert side effects
        Assert.assertEquals(",", objectUnderTest.m_string_value);
    }

    // Test written by Diffblue Cover.
    @Test
    public void parse_valueInputZeroNotNullPositiveNotNullOutputIllegalArgumentException() {

        // Arrange
        final int type = 0;
        final LogBuffer buffer = new LogBuffer();
        final long len = 4_294_967_297L;
        final String charsetName = "3";

        // Act
        thrown.expect(IllegalArgumentException.class);
        JsonConversion.parse_value(type, buffer, len, charsetName);

        // Method is not expected to return due to exception thrown
    }
}
