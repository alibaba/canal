package com.alibaba.otter.canal.protocol.position;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Regression test for overflow in EntryPosition.compareTo when position
 * difference exceeds Integer.MAX_VALUE, causing sign truncation.
 *
 * Bug: {@code return (int)(position - o.position);} casts a long difference
 * to int. When the difference exceeds 2^31, the cast truncates and may flip
 * the sign, returning the wrong comparison result.
 */
public class EntryPositionCompareToOverflowTest {

    @Test
    public void testCompareToWhenDifferenceExceedsIntRange() {
        // Create two EntryPositions whose positions differ by more than
        // Integer.MAX_VALUE (2,147,483,647).
        //
        // p2.position = 0L, p1.position = 3,000,000,000L
        //   3,000,000,000 > 0, so p1 should compare GREATER than p2.
        //   p1.compareTo(p2) should return a positive value.
        //
        // Buggy code: (int)(3_000_000_000L - 0L)
        //   3,000,000,000 in hex = 0xB2D05E00
        //   cast to int = 0xB2D05E00 which is NEGATIVE (-1294967296)
        //   WRONG SIGN!

        EntryPosition p1 = new EntryPosition("mysql-bin.000001", 3_000_000_000L);
        EntryPosition p2 = new EntryPosition("mysql-bin.000001", 0L);

        // p1.position (3 billion) > p2.position (0), so compareTo must be > 0
        int result = p1.compareTo(p2);
        assertTrue(
            String.format("Expected positive (p1.position=%d > p2.position=%d) but got %d",
                p1.getPosition(), p2.getPosition(), result),
            result > 0);
    }

    @Test
    public void testCompareToReverseOrder() {
        // p1.position = 0L, p2.position = 3,000,000,000L
        //   p1 should compare LESS than p2 (negative result).
        //
        // Buggy code: (int)(0L - 3_000_000_000L) = (int)(-3_000_000_000L)
        //   -3,000,000,000 in two's complement 64-bit = 0x4D2FA200
        //   cast to int = 0x4D2FA200 which is POSITIVE (1294967296)
        //   WRONG SIGN!

        EntryPosition p1 = new EntryPosition("mysql-bin.000001", 0L);
        EntryPosition p2 = new EntryPosition("mysql-bin.000001", 3_000_000_000L);

        // p1.position (0) < p2.position (3 billion), so compareTo must be < 0
        int result = p1.compareTo(p2);
        assertTrue(
            String.format("Expected negative (p1.position=%d < p2.position=%d) but got %d",
                p1.getPosition(), p2.getPosition(), result),
            result < 0);
    }
}
