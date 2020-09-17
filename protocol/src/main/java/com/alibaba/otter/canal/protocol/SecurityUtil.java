package com.alibaba.otter.canal.protocol;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * <pre>
 * 1、client
 *    stage1_hash = SHA1(明文密码).
 *    token = SHA1(scramble + SHA1(stage1_hash)) XOR stage1_hash
 * 2. server
 *    token = SHA1(token XOR SHA1(scramble + password))
 * 3. checktoken vs password
 * </pre>
 * 
 * @author agapple 2019年8月26日 下午4:58:15
 * @since 1.1.4
 */
public class SecurityUtil {

    private static char[]                  digits  = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c',
            'd', 'e', 'f'                         };

    private static Map<Character, Integer> rDigits = new HashMap<>(16);
    static {
        for (int i = 0; i < digits.length; ++i) {
            rDigits.put(digits[i], i);
        }
    }

    public static String md5String(String content) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("md5");
        byte[] bt = md.digest(content.getBytes());
        if (null == bt || bt.length != 16) {
            throw new IllegalArgumentException("md5 need");
        }
        return byte2HexStr(bt);
    }

    public static final String scrambleGenPass(byte[] pass) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        byte[] pass1 = md.digest(pass);
        md.reset();
        byte[] pass2 = md.digest(pass1);
        return SecurityUtil.byte2HexStr(pass2);
    }

    /**
     * server auth check
     */
    public static final boolean scrambleServerAuth(byte[] token, byte[] pass, byte[] seed)
                                                                                          throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        md.update(seed);
        byte[] pass1 = md.digest(pass);
        for (int i = 0; i < pass1.length; i++) {
            pass1[i] = (byte) (token[i] ^ pass1[i]);
        }

        md = MessageDigest.getInstance("SHA-1");
        byte[] pass2 = md.digest(pass1);
        return Arrays.equals(pass, pass2);
    }

    public static final byte[] scramble411(byte[] pass, byte[] seed) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        byte[] pass1 = md.digest(pass);
        md.reset();
        byte[] pass2 = md.digest(pass1);
        md.reset();
        md.update(seed);
        byte[] pass3 = md.digest(pass2);
        for (int i = 0; i < pass3.length; i++) {
            pass3[i] = (byte) (pass3[i] ^ pass1[i]);
        }
        return pass3;
    }

    /**
     * bytes转换成十六进制字符串
     */
    public static String byte2HexStr(byte[] b) {
        StringBuilder hs = new StringBuilder();
        for (byte value : b) {
            String hex = (Integer.toHexString(value & 0XFF));
            if (hex.length() == 1) {
                hs.append("0" + hex);
            } else {
                hs.append(hex);
            }
        }

        return hs.toString();
    }

    /**
     * bytes转换成十六进制字符串
     */
    public static byte[] hexStr2Bytes(String src) {
        if (src == null) {
            return null;
        }
        int offset = 0;
        int length = src.length();
        if (length == 0) {
            return new byte[0];
        }

        boolean odd = length << 31 == Integer.MIN_VALUE;
        byte[] bs = new byte[odd ? (length + 1) >> 1 : length >> 1];
        for (int i = offset, limit = offset + length; i < limit; ++i) {
            char high, low;
            if (i == offset && odd) {
                high = '0';
                low = src.charAt(i);
            } else {
                high = src.charAt(i);
                low = src.charAt(++i);
            }
            int b;
            switch (high) {
                case '0':
                    b = 0;
                    break;
                case '1':
                    b = 0x10;
                    break;
                case '2':
                    b = 0x20;
                    break;
                case '3':
                    b = 0x30;
                    break;
                case '4':
                    b = 0x40;
                    break;
                case '5':
                    b = 0x50;
                    break;
                case '6':
                    b = 0x60;
                    break;
                case '7':
                    b = 0x70;
                    break;
                case '8':
                    b = 0x80;
                    break;
                case '9':
                    b = 0x90;
                    break;
                case 'a':
                case 'A':
                    b = 0xa0;
                    break;
                case 'b':
                case 'B':
                    b = 0xb0;
                    break;
                case 'c':
                case 'C':
                    b = 0xc0;
                    break;
                case 'd':
                case 'D':
                    b = 0xd0;
                    break;
                case 'e':
                case 'E':
                    b = 0xe0;
                    break;
                case 'f':
                case 'F':
                    b = 0xf0;
                    break;
                default:
                    throw new IllegalArgumentException("illegal hex-string: " + src);
            }
            switch (low) {
                case '0':
                    break;
                case '1':
                    b += 1;
                    break;
                case '2':
                    b += 2;
                    break;
                case '3':
                    b += 3;
                    break;
                case '4':
                    b += 4;
                    break;
                case '5':
                    b += 5;
                    break;
                case '6':
                    b += 6;
                    break;
                case '7':
                    b += 7;
                    break;
                case '8':
                    b += 8;
                    break;
                case '9':
                    b += 9;
                    break;
                case 'a':
                case 'A':
                    b += 10;
                    break;
                case 'b':
                case 'B':
                    b += 11;
                    break;
                case 'c':
                case 'C':
                    b += 12;
                    break;
                case 'd':
                case 'D':
                    b += 13;
                    break;
                case 'e':
                case 'E':
                    b += 14;
                    break;
                case 'f':
                case 'F':
                    b += 15;
                    break;
                default:
                    throw new IllegalArgumentException("illegal hex-string: " + src);
            }
            bs[(i - offset) >> 1] = (byte) b;
        }
        return bs;
    }

}
