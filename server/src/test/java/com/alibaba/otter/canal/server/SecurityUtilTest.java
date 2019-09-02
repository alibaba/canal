package com.alibaba.otter.canal.server;

import java.security.NoSuchAlgorithmException;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.otter.canal.protocol.SecurityUtil;

public class SecurityUtilTest {

    @Test
    public void testSimple() throws NoSuchAlgorithmException {
        byte[] seed = { 1, 2, 3, 4, 5, 6, 7, 8 };
        // String str = "e3619321c1a937c46a0d8bd1dac39f93b27d4458"; // canal
        // passwd
        String str = SecurityUtil.scrambleGenPass("canal".getBytes());
        byte[] client = SecurityUtil.scramble411("canal".getBytes(), seed);
        boolean check = SecurityUtil.scrambleServerAuth(client, SecurityUtil.hexStr2Bytes(str), seed);
        Assert.assertTrue(check);
    }
}
