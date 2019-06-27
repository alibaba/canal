package com.alibaba.otter.canal.parse.driver.mysql.utils;

import java.security.DigestException;
import java.security.NoSuchAlgorithmException;

import org.junit.Assert;
import org.junit.Test;

public class MySQLPasswordEncrypterTest {

    @Test
    public void testScrambleCachingSha2() throws DigestException {
        byte[] bytes1 = new byte[]{73, -38, 6, -106, 14, -28, -98, -32,
                -80, -49, -88, -66, -116, -101, -86, 25, -7, 32, 44, -118,
                24, -128, -8, 12, 10, -38, 111, -11, 42, -111, 43, -123};

        byte[] bytes2 = new byte[]{-86, 63, -63, 80, 93, 3, 105, -59, 71,
                -41, 81, 112, 35, -29, 28, -115, -68, 16, -119, -60, -53,
                -80, -4, -19, 60, -37, 27, -22, -23, -23, 49, -36};

        Assert.assertArrayEquals(bytes1, MySQLPasswordEncrypter
                .scrambleCachingSha2(new byte[0], new byte[0]));
        Assert.assertArrayEquals(bytes2, MySQLPasswordEncrypter
                .scrambleCachingSha2(
                        new byte[]{1, 2, 3, 4, 5, 6, 7, 8}, new byte[]{1, 1}));
    }

    @Test
    public void testScramble411() throws NoSuchAlgorithmException {
        byte[] bytes1 = new byte[]{90, 11, -19, 60, 27, -27, 22, 92,
                -38, 4, 40, -62, -100, 74, 17, 6, 115, -37, -119, -126};

        byte[] bytes2 = new byte[]{-101, -23, 45, 38, -113, 65, -12,
                -55, 106, 25, -88, 107, 66, 59, -104, 11, -63, 110, -23, 83};

        Assert.assertArrayEquals(bytes1, MySQLPasswordEncrypter
                .scramble411(new byte[0], new byte[0]));
        Assert.assertArrayEquals(bytes2, MySQLPasswordEncrypter
                .scramble411(
                        new byte[]{1, 2, 3, 4, 5, 6, 7, 8}, new byte[]{1, 1}));
    }

    @Test
    public void testScramble323() {
        Assert.assertNull(MySQLPasswordEncrypter.scramble323(null, "foo"));

        Assert.assertEquals("", MySQLPasswordEncrypter.scramble323("", "foo"));
        Assert.assertEquals("X",
                MySQLPasswordEncrypter.scramble323("bar123\tbaz", "a"));
    }
}
