package com.alibaba.otter.canal.parse.driver.mysql.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public abstract class ByteHelper {

    public static final long NULL_LENGTH = -1;

    public static byte[] readNullTerminatedBytes(byte[] data, int index) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = index; i < data.length; i++) {
            byte item = data[i];
            if (item == MSC.NULL_TERMINATED_STRING_DELIMITER) {
                break;
            }
            out.write(item);
        }
        return out.toByteArray();
    }

    public static void writeNullTerminatedString(String str, ByteArrayOutputStream out) throws IOException {
        out.write(str.getBytes());
        out.write(MSC.NULL_TERMINATED_STRING_DELIMITER);
    }

    public static void writeNullTerminated(byte[] data, ByteArrayOutputStream out) throws IOException {
        out.write(data);
        out.write(MSC.NULL_TERMINATED_STRING_DELIMITER);
    }

    public static byte[] readFixedLengthBytes(byte[] data, int index, int length) {
        byte[] bytes = new byte[length];
        System.arraycopy(data, index, bytes, 0, length);
        return bytes;
    }

    /**
     * Read 4 bytes in Little-endian byte order.
     * 
     * @param data, the original byte array
     * @param index, start to read from.
     * @return
     */
    public static long readUnsignedIntLittleEndian(byte[] data, int index) {
        long result = (long) (data[index] & 0xFF) | (long) ((data[index + 1] & 0xFF) << 8)
                      | (long) ((data[index + 2] & 0xFF) << 16) | (long) ((data[index + 3] & 0xFF) << 24);
        return result;
    }

    public static long readUnsignedLongLittleEndian(byte[] data, int index) {
        long accumulation = 0;
        int position = index;
        for (int shiftBy = 0; shiftBy < 64; shiftBy += 8) {
            accumulation |= (long) ((data[position++] & 0xff) << shiftBy);
        }
        return accumulation;
    }

    public static int readUnsignedShortLittleEndian(byte[] data, int index) {
        int result = (data[index] & 0xFF) | ((data[index + 1] & 0xFF) << 8);
        return result;
    }

    public static int readUnsignedMediumLittleEndian(byte[] data, int index) {
        int result = (data[index] & 0xFF) | ((data[index + 1] & 0xFF) << 8) | ((data[index + 2] & 0xFF) << 16);
        return result;
    }

    public static long readLengthCodedBinary(byte[] data, int index) throws IOException {
        int firstByte = data[index] & 0xFF;
        switch (firstByte) {
            case 251:
                return NULL_LENGTH;
            case 252:
                return readUnsignedShortLittleEndian(data, index + 1);
            case 253:
                return readUnsignedMediumLittleEndian(data, index + 1);
            case 254:
                return readUnsignedLongLittleEndian(data, index + 1);
            default:
                return firstByte;
        }
    }

    public static byte[] readBinaryCodedLengthBytes(byte[] data, int index) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(data[index]);

        byte[] buffer = null;
        int value = data[index] & 0xFF;
        if (value == 251) {
            buffer = new byte[0];
        }
        if (value == 252) {
            buffer = new byte[2];
        }
        if (value == 253) {
            buffer = new byte[3];
        }
        if (value == 254) {
            buffer = new byte[8];
        }
        if (buffer != null) {
            System.arraycopy(data, index + 1, buffer, 0, buffer.length);
            out.write(buffer);
        }

        return out.toByteArray();
    }

    public static void write8ByteUnsignedIntLittleEndian(long data, ByteArrayOutputStream out) {
        out.write((byte) (data & 0xFF));
        out.write((byte) (data >>> 8));
        out.write((byte) (data >>> 16));
        out.write((byte) (data >>> 24));
        out.write((byte) (data >>> 32));
        out.write((byte) (data >>> 40));
        out.write((byte) (data >>> 48));
        out.write((byte) (data >>> 56));
    }

    public static void writeUnsignedIntLittleEndian(long data, ByteArrayOutputStream out) {
        out.write((byte) (data & 0xFF));
        out.write((byte) (data >>> 8));
        out.write((byte) (data >>> 16));
        out.write((byte) (data >>> 24));
    }

    public static void writeUnsignedInt64LittleEndian(long data, ByteArrayOutputStream out) {
        out.write((byte) (data & 0xFF));
        out.write((byte) (data >>> 8));
        out.write((byte) (data >>> 16));
        out.write((byte) (data >>> 24));
        out.write((byte) (data >>> 32));
        out.write((byte) (data >>> 40));
        out.write((byte) (data >>> 48));
        out.write((byte) (data >>> 56));
    }

    public static void writeUnsignedShortLittleEndian(int data, ByteArrayOutputStream out) {
        out.write((byte) (data & 0xFF));
        out.write((byte) ((data >>> 8) & 0xFF));
    }

    public static void writeUnsignedMediumLittleEndian(int data, ByteArrayOutputStream out) {
        out.write((byte) (data & 0xFF));
        out.write((byte) ((data >>> 8) & 0xFF));
        out.write((byte) ((data >>> 16) & 0xFF));
    }

    public static void writeBinaryCodedLengthBytes(byte[] data, ByteArrayOutputStream out) throws IOException {
        // 1. write length byte/bytes
        if (data.length < 252) {
            out.write((byte) data.length);
        } else if (data.length < (1 << 16L)) {
            out.write((byte) 252);
            writeUnsignedShortLittleEndian(data.length, out);
        } else if (data.length < (1 << 24L)) {
            out.write((byte) 253);
            writeUnsignedMediumLittleEndian(data.length, out);
        } else {
            out.write((byte) 254);
            writeUnsignedIntLittleEndian(data.length, out);
        }
        // 2. write real data followed length byte/bytes
        out.write(data);
    }

    public static void writeFixedLengthBytes(byte[] data, int index, int length, ByteArrayOutputStream out) {
        for (int i = index; i < index + length; i++) {
            out.write(data[i]);
        }
    }

    public static void writeFixedLengthBytesFromStart(byte[] data, int length, ByteArrayOutputStream out) {
        writeFixedLengthBytes(data, 0, length, out);
    }

}
