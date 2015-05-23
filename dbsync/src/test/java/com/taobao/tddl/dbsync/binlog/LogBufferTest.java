package com.taobao.tddl.dbsync.binlog;

import java.math.BigDecimal;
import java.math.BigInteger;

import junit.framework.TestCase;

public class LogBufferTest extends TestCase {

    public static final int LOOP = 10000;

    public void testSigned() {
        byte[] array = { 0, 0, 0, (byte) 0xff };

        LogBuffer buffer = new LogBuffer(array, 0, array.length);

        System.out.println(buffer.getInt32(0));
        System.out.println(buffer.getUint32(0));

        System.out.println(buffer.getInt24(1));
        System.out.println(buffer.getUint24(1));
    }

    public void testBigInteger() {
        byte[] array = { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
                (byte) 0xff };

        LogBuffer buffer = new LogBuffer(array, 0, array.length);

        long tt1 = 0;
        long l1 = 0;
        for (int i = 0; i < LOOP; i++) {
            final long t1 = System.nanoTime();
            l1 = buffer.getLong64(0);
            tt1 += System.nanoTime() - t1;
        }
        System.out.print(tt1 / LOOP);
        System.out.print("ns >> ");
        System.out.println(l1);

        long tt2 = 0;
        BigInteger l2 = null;
        for (int i = 0; i < LOOP; i++) {
            final long t2 = System.nanoTime();
            l2 = buffer.getUlong64(0);
            tt2 += System.nanoTime() - t2;
        }
        System.out.print(tt2 / LOOP);
        System.out.print("ns >> ");
        System.out.println(l2);
    }

    /* Reads big-endian integer from no more than 4 bytes */
    private static int convertNBytesToInt(byte[] buffer, int offset, int length) {
        int ret = 0;
        for (int i = offset; i < (offset + length); i++) {
            ret = (ret << 8) | (0xff & buffer[i]);
        }
        return ret;
    }

    private static int convert4BytesToInt(byte[] buffer, int offset) {
        int value;
        value = (0xff & buffer[offset + 3]);
        value += (0xff & buffer[offset + 2]) << 8;
        value += (0xff & buffer[offset + 1]) << 16;
        value += (0xff & buffer[offset]) << 24;
        return value;
    }

    public static short convert1ByteToShort(byte[] buffer, int offset) {
        short value;
        value = (short) buffer[offset + 0];
        return value;
    }

    public static short convert2bytesToShort(byte[] buffer, int offset) {
        short value;
        value = (short) (buffer[offset + 0] << 8);
        value += (short) (buffer[offset + 1] & 0xff);
        return value;
    }

    public static final BigDecimal extractDecimal(byte[] buffer, int precision, int scale) {
        //
        // Decimal representation in binlog seems to be as follows:
        // 1 byte - 'precision'
        // 1 byte - 'scale'
        // remaining n bytes - integer such that value = n / (10^scale)
        // Integer is represented as follows:
        // 1st bit - sign such that set == +, unset == -
        // every 4 bytes represent 9 digits in big-endian order, so that if
        // you print the values of these quads as big-endian integers one after
        // another, you get the whole number string representation in decimal.
        // What remains is to put a sign and a decimal dot.
        // 13 0a 80 00 00 05 1b 38 b0 60 00 means:
        // 0x13 - precision = 19
        // 0x0a - scale = 10
        // 0x80 - positive
        // 0x00000005 0x1b38b060 0x00
        // 5 456700000 0
        // 54567000000 / 10^{10} = 5.4567
        //
        // int_size below shows how long is integer part
        //
        // offset = offset + 2; // offset of the number part
        //
        int intg = precision - scale;
        int intg0 = intg / LogBuffer.DIG_PER_INT32;
        int frac0 = scale / LogBuffer.DIG_PER_INT32;
        int intg0x = intg - intg0 * LogBuffer.DIG_PER_INT32;
        int frac0x = scale - frac0 * LogBuffer.DIG_PER_INT32;

        int offset = 0;

        int sign = (buffer[offset] & 0x80) == 0x80 ? 1 : -1;

        // how many bytes are used to represent given amount of digits?
        int integerSize = intg0 * LogBuffer.SIZE_OF_INT32 + LogBuffer.dig2bytes[intg0x];
        int decimalSize = frac0 * LogBuffer.SIZE_OF_INT32 + LogBuffer.dig2bytes[frac0x];

        int bin_size = integerSize + decimalSize; // total bytes
        byte[] d_copy = new byte[bin_size];

        if (bin_size > buffer.length) {
            throw new ArrayIndexOutOfBoundsException("Calculated bin_size: " + bin_size + ", available bytes: "
                                                     + buffer.length);
        }

        // Invert first bit
        d_copy[0] = buffer[0];
        d_copy[0] ^= 0x80;
        if (sign == -1) {
            // Invert every byte
            d_copy[0] ^= 0xFF;
        }

        for (int i = 1; i < bin_size; i++) {
            d_copy[i] = buffer[i];
            if (sign == -1) {
                // Invert every byte
                d_copy[i] ^= 0xFF;
            }
        }

        // Integer part
        offset = LogBuffer.dig2bytes[intg0x];

        BigDecimal intPart = new BigDecimal(0);

        if (offset > 0) intPart = BigDecimal.valueOf(convertNBytesToInt(d_copy, 0, offset));

        while (offset < integerSize) {
            intPart = intPart.movePointRight(LogBuffer.DIG_PER_DEC1).add(BigDecimal.valueOf(convert4BytesToInt(d_copy,
                offset)));
            offset += 4;
        }

        // Decimal part
        BigDecimal fracPart = new BigDecimal(0);
        int shift = 0;
        for (int i = 0; i < frac0; i++) {
            shift += LogBuffer.DIG_PER_DEC1;
            fracPart = fracPart.add(BigDecimal.valueOf(convert4BytesToInt(d_copy, offset)).movePointLeft(shift));
            offset += 4;
        }

        if (LogBuffer.dig2bytes[frac0x] > 0) {
            fracPart = fracPart.add(BigDecimal.valueOf(convertNBytesToInt(d_copy, offset, LogBuffer.dig2bytes[frac0x]))
                .movePointLeft(shift + frac0x));
        }

        return BigDecimal.valueOf(sign).multiply(intPart.add(fracPart));
    }

    public static final byte[] array1 = { (byte) 0x80, 0x00, 0x00, 0x05, 0x1b, 0x38, (byte) 0xb0, 0x60, 0x00 };

    public static final byte[] array2 = { (byte) 0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xfb, (byte) 0xe4, (byte) 0xc7,
            (byte) 0x4f, (byte) 0xa0, (byte) 0xff };

    public static final byte[] array3 = { -128, 0, 6, 20, 113, 56, 6, 26, -123 };

    public static final byte[] array4 = { -128, 7, 0, 0, 0, 1, 0, 0, 3 };

    public static final byte[] array5 = { -128, 0, 0, 0, 0, 1, 1, -122, -96, -108 };

    public void testBigDecimal() throws InterruptedException {
        do {
            System.out.println("old extract decimal: ");

            long tt1 = 0;
            BigDecimal bd1 = null;
            for (int i = 0; i < LOOP; i++) {
                final long t1 = System.nanoTime();
                bd1 = extractDecimal(array2, 19, 10);
                tt1 += System.nanoTime() - t1;
            }
            System.out.print(tt1 / LOOP);
            System.out.print("ns >> ");
            System.out.println(bd1);

            long tt2 = 0;
            BigDecimal bd2 = null;
            for (int i = 0; i < LOOP; i++) {
                final long t2 = System.nanoTime();
                bd2 = extractDecimal(array1, 19, 10);
                tt2 += System.nanoTime() - t2;
            }
            System.out.print(tt2 / LOOP);
            System.out.print("ns >> ");
            System.out.println(bd2);

            long tt3 = 0;
            BigDecimal bd3 = null;
            for (int i = 0; i < LOOP; i++) {
                final long t3 = System.nanoTime();
                bd3 = extractDecimal(array3, 18, 6);
                tt3 += System.nanoTime() - t3;
            }
            System.out.print(tt3 / LOOP);
            System.out.print("ns >> ");
            System.out.println(bd3);

            long tt4 = 0;
            BigDecimal bd4 = null;
            for (int i = 0; i < LOOP; i++) {
                final long t4 = System.nanoTime();
                bd4 = extractDecimal(array4, 18, 6);
                tt4 += System.nanoTime() - t4;
            }
            System.out.print(tt4 / LOOP);
            System.out.print("ns >> ");
            System.out.println(bd4);

            long tt5 = 0;
            BigDecimal bd5 = null;
            for (int i = 0; i < LOOP; i++) {
                final long t5 = System.nanoTime();
                bd5 = extractDecimal(array5, 18, 6);
                tt5 += System.nanoTime() - t5;
            }
            System.out.print(tt5 / LOOP);
            System.out.print("ns >> ");
            System.out.println(bd5);
        } while (false);

        do {
            System.out.println("new extract decimal: ");

            LogBuffer buffer1 = new LogBuffer(array2, 0, array2.length);
            LogBuffer buffer2 = new LogBuffer(array1, 0, array1.length);
            LogBuffer buffer3 = new LogBuffer(array3, 0, array3.length);
            LogBuffer buffer4 = new LogBuffer(array4, 0, array4.length);
            LogBuffer buffer5 = new LogBuffer(array5, 0, array5.length);

            long tt1 = 0;
            BigDecimal bd1 = null;
            for (int i = 0; i < LOOP; i++) {
                final long t1 = System.nanoTime();
                bd1 = buffer1.getDecimal(0, 19, 10);
                tt1 += System.nanoTime() - t1;
            }
            System.out.print(tt1 / LOOP);
            System.out.print("ns >> ");
            System.out.println(bd1);

            long tt2 = 0;
            BigDecimal bd2 = null;
            for (int i = 0; i < LOOP; i++) {
                final long t2 = System.nanoTime();
                bd2 = buffer2.getDecimal(0, 19, 10);
                tt2 += System.nanoTime() - t2;
            }
            System.out.print(tt2 / LOOP);
            System.out.print("ns >> ");
            System.out.println(bd2);

            long tt3 = 0;
            BigDecimal bd3 = null;
            for (int i = 0; i < LOOP; i++) {
                final long t3 = System.nanoTime();
                bd3 = buffer3.getDecimal(0, 18, 6);
                tt3 += System.nanoTime() - t3;
            }
            System.out.print(tt3 / LOOP);
            System.out.print("ns >> ");
            System.out.println(bd3);

            long tt4 = 0;
            BigDecimal bd4 = null;
            for (int i = 0; i < LOOP; i++) {
                final long t4 = System.nanoTime();
                bd4 = buffer4.getDecimal(0, 18, 6);
                tt4 += System.nanoTime() - t4;
            }
            System.out.print(tt4 / LOOP);
            System.out.print("ns >> ");
            System.out.println(bd4);

            long tt5 = 0;
            BigDecimal bd5 = null;
            for (int i = 0; i < LOOP; i++) {
                final long t5 = System.nanoTime();
                bd5 = buffer5.getDecimal(0, 18, 6);
                tt5 += System.nanoTime() - t5;
            }
            System.out.print(tt5 / LOOP);
            System.out.print("ns >> ");
            System.out.println(bd5);
        } while (false);
    }
}
