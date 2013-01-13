package com.alibaba.otter.canal.parse.inbound.mysql.networking.utils;

import java.io.IOException;

import org.apache.commons.lang.ArrayUtils;

import com.alibaba.otter.canal.parse.support.ByteHelper;

public class LengthCodedStringReader {

    public static final String CODE_PAGE_1252 = "Cp1252";

    private String             encoding;
    private int                index          = 0;       // 数组下标

    public LengthCodedStringReader(String encoding, int startIndex){
        this.encoding = encoding;
        this.index = startIndex;
    }

    public String readLengthCodedString(byte[] data) throws IOException {
        byte[] lengthBytes = ByteHelper.readBinaryCodedLengthBytes(data, getIndex());
        long length = ByteHelper.readLengthCodedBinary(data, getIndex());
        setIndex(getIndex() + lengthBytes.length);
        try {
            return new String(ArrayUtils.subarray(data, getIndex(), (int) (getIndex() + length)),
                              encoding == null ? CODE_PAGE_1252 : encoding);
        } finally {
            setIndex((int) (getIndex() + length));
        }

    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getIndex() {
        return index;
    }
}
