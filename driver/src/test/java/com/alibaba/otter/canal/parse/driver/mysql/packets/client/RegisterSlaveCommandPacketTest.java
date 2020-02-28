package com.alibaba.otter.canal.parse.driver.mysql.packets.client;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

public class RegisterSlaveCommandPacketTest {

    @Rule
    public final ExpectedException thrown        = ExpectedException.none();

    @SuppressWarnings("deprecation")
    @Rule
    public final Timeout           globalTimeout = new Timeout(10000);

    /* testedClasses: RegisterSlaveCommandPacket */
    // Test written by Diffblue Cover.
    @Test
    public void toBytesOutput27() throws IOException, InvocationTargetException {

        // Arrange
        final RegisterSlaveCommandPacket objectUnderTest = new RegisterSlaveCommandPacket();
        objectUnderTest.serverId = 0L;
        objectUnderTest.reportPort = 0;
        objectUnderTest.reportPasswd = "foo";
        objectUnderTest.reportHost = "foo";
        objectUnderTest.reportUser = "foo";
        objectUnderTest.setCommand((byte) 0);

        // Act
        final byte[] actual = objectUnderTest.toBytes();

        // Assert result
        Assert.assertArrayEquals(new byte[] { (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 3, (byte) 102,
                (byte) 111, (byte) 111, (byte) 3, (byte) 102, (byte) 111, (byte) 111, (byte) 3, (byte) 102, (byte) 111,
                (byte) 111, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0,
                (byte) 0 }, actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void toLHInputZeroOutput4() {

        // Arrange
        final int n = 0;

        // Act
        final byte[] actual = RegisterSlaveCommandPacket.toLH(n);

        // Assert result
        Assert.assertArrayEquals(new byte[] { (byte) 0, (byte) 0, (byte) 0, (byte) 0 }, actual);
    }
}
