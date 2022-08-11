package com.taobao.tddl.dbsync.binlog.event;

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

public class LogHeaderTest {

    @Rule
    public final ExpectedException thrown        = ExpectedException.none();

    @SuppressWarnings("deprecation")
    @Rule
    public final Timeout           globalTimeout = new Timeout(10000);

    /* testedClasses: LogHeader */
    // Test written by Diffblue Cover.

    @Test
    public void constructorInputZeroOutputVoid() {

        // Arrange
        final int type = 0;

        // Act, creating object to test constructor
        final LogHeader objectUnderTest = new LogHeader(type);

        // Assert side effects
        final HashMap<String, String> hashMap = new HashMap<>();
        Assert.assertEquals(hashMap, objectUnderTest.gtidMap);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getChecksumAlgOutputZero() {

        // Arrange
        final LogHeader objectUnderTest = new LogHeader(0);

        // Act
        final int actual = objectUnderTest.getChecksumAlg();

        // Assert result
        Assert.assertEquals(0, actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getCrcOutputZero() {

        // Arrange
        final LogHeader objectUnderTest = new LogHeader(0);

        // Act
        final long actual = objectUnderTest.getCrc();

        // Assert result
        Assert.assertEquals(0L, actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getCurrentGtidLastCommitOutputNull() {

        // Arrange
        final LogHeader objectUnderTest = new LogHeader(0);

        // Act
        final String actual = objectUnderTest.getCurrentGtidLastCommit();

        // Assert result
        Assert.assertNull(actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getCurrentGtidOutputNull() {

        // Arrange
        final LogHeader objectUnderTest = new LogHeader(0);

        // Act
        final String actual = objectUnderTest.getCurrentGtid();

        // Assert result
        Assert.assertNull(actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getCurrentGtidSnOutputNull() {

        // Arrange
        final LogHeader objectUnderTest = new LogHeader(0);

        // Act
        final String actual = objectUnderTest.getCurrentGtidSn();

        // Assert result
        Assert.assertNull(actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getEventLenOutputZero() {

        // Arrange
        final LogHeader objectUnderTest = new LogHeader(0);

        // Act
        final int actual = objectUnderTest.getEventLen();

        // Assert result
        Assert.assertEquals(0, actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getFlagsOutputZero() {

        // Arrange
        final LogHeader objectUnderTest = new LogHeader(0);

        // Act
        final int actual = objectUnderTest.getFlags();

        // Assert result
        Assert.assertEquals(0, actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getGtidSetStrOutputNull() {

        // Arrange
        final LogHeader objectUnderTest = new LogHeader(0);

        // Act
        final String actual = objectUnderTest.getGtidSetStr();

        // Assert result
        Assert.assertNull(actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getLogFileNameOutputNull() {

        // Arrange
        final LogHeader objectUnderTest = new LogHeader(0);

        // Act
        final String actual = objectUnderTest.getLogFileName();

        // Assert result
        Assert.assertNull(actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getLogPosOutputZero() {

        // Arrange
        final LogHeader objectUnderTest = new LogHeader(0);

        // Act
        final long actual = objectUnderTest.getLogPos();

        // Assert result
        Assert.assertEquals(0L, actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getServerIdOutputZero() {

        // Arrange
        final LogHeader objectUnderTest = new LogHeader(0);

        // Act
        final long actual = objectUnderTest.getServerId();

        // Assert result
        Assert.assertEquals(0L, actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeOutputZero() {

        // Arrange
        final LogHeader objectUnderTest = new LogHeader(0);

        // Act
        final int actual = objectUnderTest.getType();

        // Assert result
        Assert.assertEquals(0, actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getWhenOutputZero() {

        // Arrange
        final LogHeader objectUnderTest = new LogHeader(0);

        // Act
        final long actual = objectUnderTest.getWhen();

        // Assert result
        Assert.assertEquals(0L, actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void setLogFileNameInputNotNullOutputVoid() {

        // Arrange
        final LogHeader objectUnderTest = new LogHeader(0);
        final String logFileName = "3";

        // Act
        objectUnderTest.setLogFileName(logFileName);

        // Assert side effects
        Assert.assertEquals("3", objectUnderTest.getLogFileName());
    }
}
