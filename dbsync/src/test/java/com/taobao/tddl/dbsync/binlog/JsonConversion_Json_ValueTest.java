package com.taobao.tddl.dbsync.binlog;

import java.nio.charset.Charset;

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

    // test for 5127
    @Test
    public void testJsonKeyContainsSpecialCharacter() {

        // {"internal_uri_rewrite": {"(.*)(/[^/]+\\.(mp|MP)4)$": "$1/mp4$2"}}
        String jsonData = "{\"internal_uri_rewrite\": {\"(.*)(/[^/]+\\\\.(mp|MP)4)$\": \"$1/mp4$2\"}}";
        byte[] data = new byte[] { 1, 0, 74, 0, 11, 0, 20, 0, 0, 31, 0, 105, 110, 116, 101, 114, 110, 97, 108, 95, 117,
                                   114, 105, 95, 114, 101, 119, 114, 105, 116, 101, 1, 0, 43, 0, 11, 0, 23, 0, 12, 34,
                                   0, 40, 46, 42, 41, 40, 47, 91, 94, 47, 93, 43, 92, 46, 40, 109, 112, 124, 77, 80, 41,
                                   52, 41, 36, 8, 36, 49, 47, 109, 112, 52, 36, 50 };
        final LogBuffer buffer = new LogBuffer(data, 0, 74);

        Charset charset = Charset.forName("UTF-8");
        Json_Value jsonValue = JsonConversion.parse_value(0, buffer, 74, charset);
        StringBuilder builder = new StringBuilder();
        jsonValue.toJsonString(builder, charset);

        Assert.assertEquals(builder.toString(), jsonData);

        Assert.assertEquals(jsonValue.key(0, charset), "internal_uri_rewrite");

        Json_Value element = jsonValue.element(0, charset);
        Assert.assertEquals(element.key(0, charset), "(.*)(/[^/]+\\.(mp|MP)4)$");
    }
}
