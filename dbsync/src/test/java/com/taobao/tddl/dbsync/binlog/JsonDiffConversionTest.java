package com.taobao.tddl.dbsync.binlog;

import java.lang.reflect.InvocationTargetException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

public class JsonDiffConversionTest {

    @Rule
    public final ExpectedException thrown        = ExpectedException.none();

    @SuppressWarnings("deprecation")
    @Rule
    public final Timeout           globalTimeout = new Timeout(10000);

    /* testedClasses: JsonDiffConversion */
    // Test written by Diffblue Cover.
    @Test
    public void print_json_diffInputNotNullPositiveNotNullZeroNotNullOutputIllegalArgumentException()
                                                                                                     throws InvocationTargetException {

        // Arrange
        final LogBuffer buffer = new LogBuffer();
        buffer.position = 28;
        buffer.semival = 0;
        final byte[] myByteArray = { (byte) 3, (byte) 3, (byte) 67, (byte) 67, (byte) 67, (byte) 67, (byte) 67,
                (byte) 66, (byte) 67, (byte) 66, (byte) 67, (byte) 66, (byte) 67, (byte) 67, (byte) 67, (byte) 66,
                (byte) 67, (byte) 66, (byte) 67, (byte) 66, (byte) 67, (byte) 67, (byte) 67, (byte) 66, (byte) 67,
                (byte) 67, (byte) 67, (byte) 66, (byte) 2, (byte) 66 };
        buffer.buffer = myByteArray;
        buffer.limit = -1_000_000_065;
        buffer.origin = 1_000_000_096;
        final long len = 71L;
        final String columnName = "foo";
        final int columnIndex = 0;
        final String charsetName = "foo";
        try {

            // Act
            thrown.expect(IllegalArgumentException.class);
            JsonDiffConversion.print_json_diff(buffer, len, columnName, columnIndex, charsetName);
        } catch (IllegalArgumentException ex) {

            // Assert side effects
            Assert.assertNotNull(buffer);
            Assert.assertEquals(30, buffer.position);
            Assert.assertEquals(0, buffer.semival);
            Assert.assertArrayEquals(new byte[] { (byte) 3, (byte) 3, (byte) 67, (byte) 67, (byte) 67, (byte) 67,
                    (byte) 67, (byte) 66, (byte) 67, (byte) 66, (byte) 67, (byte) 66, (byte) 67, (byte) 67, (byte) 67,
                    (byte) 66, (byte) 67, (byte) 66, (byte) 67, (byte) 66, (byte) 67, (byte) 67, (byte) 67, (byte) 66,
                    (byte) 67, (byte) 67, (byte) 67, (byte) 66, (byte) 2, (byte) 66 }, buffer.buffer);
            Assert.assertEquals(-1_000_000_065, buffer.limit);
            Assert.assertEquals(1_000_000_096, buffer.origin);
            throw ex;
        }
    }

    // Test written by Diffblue Cover.
    @Test
    public void print_json_diffInputNotNullZeroNotNullZeroNotNullOutputIllegalArgumentException()
                                                                                                 throws InvocationTargetException {

        // Arrange
        final LogBuffer buffer = new LogBuffer();
        buffer.position = 28;
        buffer.semival = 0;
        final byte[] myByteArray = { (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2,
                (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2,
                (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2,
                (byte) 3, (byte) 2 };
        buffer.buffer = myByteArray;
        buffer.limit = 31;
        buffer.origin = 0;
        final long len = 0L;
        final String columnName = "foo";
        final int columnIndex = 0;
        final String charsetName = "foo";
        try {

            // Act
            thrown.expect(IllegalArgumentException.class);
            JsonDiffConversion.print_json_diff(buffer, len, columnName, columnIndex, charsetName);
        } catch (IllegalArgumentException ex) {

            // Assert side effects
            Assert.assertNotNull(buffer);
            Assert.assertEquals(29, buffer.position);
            Assert.assertEquals(0, buffer.semival);
            Assert.assertArrayEquals(new byte[] { (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2,
                    (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2,
                    (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2,
                    (byte) 2, (byte) 3, (byte) 2 }, buffer.buffer);
            Assert.assertEquals(31, buffer.limit);
            Assert.assertEquals(0, buffer.origin);
            throw ex;
        }
    }

    // Test written by Diffblue Cover.
    @Test
    public void print_json_diffInputNotNullZeroNotNullZeroNotNullOutputIllegalArgumentException2()
                                                                                                  throws InvocationTargetException {

        // Arrange
        final LogBuffer buffer = new LogBuffer();
        buffer.position = 15;
        buffer.semival = 0;
        final byte[] myByteArray = { (byte) 1, (byte) 1, (byte) 0, (byte) 0, (byte) 1, (byte) 1, (byte) 1, (byte) 1,
                (byte) 1, (byte) 1, (byte) 0, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 0, (byte) 1, (byte) 1,
                (byte) 0, (byte) 0, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 0,
                (byte) 1, (byte) 1 };
        buffer.buffer = myByteArray;
        buffer.limit = -1_215_751_986;
        buffer.origin = 1_215_752_002;
        final long len = 0L;
        final String columnName = "foo";
        final int columnIndex = 0;
        final String charsetName = "foo";
        try {

            // Act
            thrown.expect(IllegalArgumentException.class);
            JsonDiffConversion.print_json_diff(buffer, len, columnName, columnIndex, charsetName);
        } catch (IllegalArgumentException ex) {

            // Assert side effects
            Assert.assertNotNull(buffer);
            Assert.assertEquals(16, buffer.position);
            Assert.assertEquals(0, buffer.semival);
            Assert.assertArrayEquals(new byte[] { (byte) 1, (byte) 1, (byte) 0, (byte) 0, (byte) 1, (byte) 1, (byte) 1,
                    (byte) 1, (byte) 1, (byte) 1, (byte) 0, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 0, (byte) 1,
                    (byte) 1, (byte) 0, (byte) 0, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1,
                    (byte) 0, (byte) 1, (byte) 1 }, buffer.buffer);
            Assert.assertEquals(-1_215_751_986, buffer.limit);
            Assert.assertEquals(1_215_752_002, buffer.origin);
            throw ex;
        }
    }

    // Test written by Diffblue Cover.
    @Test
    public void print_json_diffInputNotNullZeroNotNullZeroNotNullOutputIllegalArgumentException3()
                                                                                                  throws InvocationTargetException {

        // Arrange
        final LogBuffer buffer = new LogBuffer();
        buffer.position = 27;
        buffer.semival = 0;
        final byte[] myByteArray = { (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0,
                (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0,
                (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0,
                (byte) 1, (byte) 0 };
        buffer.buffer = myByteArray;
        buffer.limit = 31;
        buffer.origin = -1;
        final long len = 0L;
        final String columnName = "foo";
        final int columnIndex = 0;
        final String charsetName = "foo";
        try {

            // Act
            thrown.expect(IllegalArgumentException.class);
            JsonDiffConversion.print_json_diff(buffer, len, columnName, columnIndex, charsetName);
        } catch (IllegalArgumentException ex) {

            // Assert side effects
            Assert.assertNotNull(buffer);
            Assert.assertEquals(29, buffer.position);
            Assert.assertEquals(0, buffer.semival);
            Assert.assertArrayEquals(new byte[] { (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0,
                    (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0,
                    (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0,
                    (byte) 0, (byte) 1, (byte) 0 }, buffer.buffer);
            Assert.assertEquals(31, buffer.limit);
            Assert.assertEquals(-1, buffer.origin);
            throw ex;
        }
    }

    // Test written by Diffblue Cover.
    @Test
    public void print_json_diffInputNotNullZeroNotNullZeroNotNullOutputNotNull() {

        // Arrange
        final LogBuffer buffer = new LogBuffer();
        final long len = 0L;
        final String columnName = ",";
        final int columnIndex = 0;
        final String charsetName = "1a 2b 3c";

        // Act
        final StringBuilder actual = JsonDiffConversion.print_json_diff(buffer,
            len,
            columnName,
            columnIndex,
            charsetName);

        // Assert result
        Assert.assertNotNull(actual);
        Assert.assertEquals(",", actual.toString());
    }
}
