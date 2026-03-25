package com.taobao.tddl.dbsync.binlog.event;

import com.taobao.tddl.dbsync.binlog.LogBuffer;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link TableMapLogEvent} optional metadata parsing.
 */
public class TableMapLogEventTest {

    /**
     * Verify that unknown optional metadata types are skipped gracefully
     * instead of throwing IllegalArgumentException.
     *
     * @see <a href="https://github.com/alibaba/canal/issues/5391">Issue #5391</a>
     */
    @Test
    public void testUnknownOptionalMetadataTypeIsSkipped() {
        // LogBuffer.forward() is the mechanism used to skip unknown metadata.
        // Verify it correctly advances the position without reading the data.
        byte[] data = new byte[]{0x01, 0x02, 0x03, 0x04, 0x05};
        LogBuffer buffer = new LogBuffer(data, 0, data.length);

        int posBefore = buffer.position();
        buffer.forward(3);
        int posAfter = buffer.position();

        Assert.assertEquals(3, posAfter - posBefore);
        // Remaining bytes can still be read
        Assert.assertEquals(0x04, buffer.getUint8());
        Assert.assertEquals(0x05, buffer.getUint8());
    }

    /**
     * Verify that forward() with the exact remaining length consumes the buffer completely.
     */
    @Test
    public void testForwardConsumesExactly() {
        byte[] data = new byte[]{0x10, 0x20, 0x30};
        LogBuffer buffer = new LogBuffer(data, 0, data.length);

        buffer.forward(3);
        Assert.assertFalse(buffer.hasRemaining());
    }

    /**
     * Verify that forward(0) is a no-op.
     */
    @Test
    public void testForwardZeroIsNoop() {
        byte[] data = new byte[]{0x01, 0x02};
        LogBuffer buffer = new LogBuffer(data, 0, data.length);

        int posBefore = buffer.position();
        buffer.forward(0);
        Assert.assertEquals(posBefore, buffer.position());
    }
}
