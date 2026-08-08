package com.taobao.tddl.dbsync.binlog.event;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.LogEvent;

/**
 * Regression for issue #5601: a STRING_RESULT user variable carrying an
 * out-of-range collation id (MariaDB 11.8+ / recent MySQL emit ids at or above
 * the 2048-entry lookup-table ceiling) must not abort binlog parsing.
 *
 * <p>Before the fix, {@code CharsetConversion.getNioCharset()} returned null for
 * those ids and the null {@link java.nio.charset.Charset} was passed straight to
 * {@code getFixString(len, charset)}, whose {@code new String(buf, from, len, null)}
 * threw {@link NullPointerException}. After the fix it degrades like
 * {@code QueryLogEvent} (ISO-8859-1 fallback).
 *
 * @see <a href="https://github.com/alibaba/canal/issues/5601">issue #5601</a>
 */
public class UserVarLogEventTest {

    @Test
    public void stringResultWithOverflowCharset_degradesInsteadOfNpe() throws Exception {
        FormatDescriptionLogEvent description = new FormatDescriptionLogEvent(4);
        // How many header bytes UserVarLogEvent skips before the variable data part.
        int headerLen = description.commonHeaderLen + description.postHeaderLen[LogEvent.USER_VAR_EVENT - 1];

        // Variable data part layout:
        //   nameLen(4 LE) + name + isNull(1) + type(1) + charsetNumber(4 LE) + valueLen(4 LE) + value
        String name = "v";
        byte[] nameBytes = name.getBytes(StandardCharsets.ISO_8859_1);
        String value = "hello";
        byte[] valueBytes = value.getBytes(StandardCharsets.ISO_8859_1);

        ByteBuffer var = ByteBuffer
            .wrap(new byte[4 + nameBytes.length + 1 + 1 + 4 + 4 + valueBytes.length])
            .order(ByteOrder.LITTLE_ENDIAN);
        var.putInt(nameBytes.length);
        var.put(nameBytes);
        var.put((byte) 0);                                      // isNull = false
        var.put((byte) UserVarLogEvent.STRING_RESULT);          // type = STRING_RESULT
        var.putInt(2048);                                       // charsetNumber: beyond 2048-entry table
        var.putInt(valueBytes.length);
        var.put(valueBytes);

        byte[] data = new byte[headerLen + var.array().length];
        System.arraycopy(var.array(), 0, data, headerLen, var.array().length);

        LogBuffer buffer = new LogBuffer(data, 0, data.length);
        LogHeader logHeader = new LogHeader(LogEvent.USER_VAR_EVENT);

        // Before the fix this threw NullPointerException; after the fix it degrades.
        UserVarLogEvent event = new UserVarLogEvent(logHeader, buffer, description);
        Assert.assertEquals("SET @v := 'hello'", event.getQuery());
    }
}
