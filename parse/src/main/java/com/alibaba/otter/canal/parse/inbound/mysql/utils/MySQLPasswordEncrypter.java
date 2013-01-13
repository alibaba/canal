package com.alibaba.otter.canal.parse.inbound.mysql.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MySQLPasswordEncrypter {

    /**
     * @param password
     * @param seeds
     * @return
     * @throws NoSuchAlgorithmException
     */
    public byte[] encrypt(byte[] password, byte[] seeds) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        byte[] passwordHashStage1 = md.digest(password);
        md.reset();

        byte[] passwordHashStage2 = md.digest(passwordHashStage1);
        md.reset();

        md.update(seeds);
        md.update(passwordHashStage2);
        byte[] toBeXord = md.digest();
        int numToXor = toBeXord.length;
        for (int i = 0; i < numToXor; i++) {
            toBeXord[i] = (byte) (toBeXord[i] ^ passwordHashStage1[i]);
        }
        return toBeXord;
    }
}
