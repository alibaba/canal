package com.alibaba.otter.canal.parse.driver.mysql.utils; 

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

public class ByteHelperTest {

	@Test
	public void testReadBinaryCodedLengthBytes() throws Exception {
		Assert.assertArrayEquals(new byte[] {95},
			ByteHelper.readBinaryCodedLengthBytes(new byte[] {94, 95, 96}, 1));
		Assert.assertArrayEquals(new byte[] {-5},
			ByteHelper.readBinaryCodedLengthBytes(new byte[] {-4, -5, -6}, 1));
		Assert.assertArrayEquals(new byte[] {-4, -6, -7},
			ByteHelper.readBinaryCodedLengthBytes(new byte[] {-6, -4, -6, -7, -8}, 1));
		Assert.assertArrayEquals(new byte[] {-3, -4, -5, -6},
			ByteHelper.readBinaryCodedLengthBytes(new byte[] {-2, -3, -4, -5, -6}, 1));
		Assert.assertArrayEquals(new byte[] {-2, -3, -4, -5, -6, -7, -8, -9, -10},
			ByteHelper.readBinaryCodedLengthBytes(new byte[] {-1, -2, -3, -4, -5, -6, -7, -8, -9, -10}, 1));
	}

	@Test
	public void testReadFixedLengthBytes() {
		Assert.assertArrayEquals(new byte[] {}, ByteHelper.readFixedLengthBytes(new byte[0], 0, 0));
	}

	@Test
	public void testReadLengthCodedBinary() throws IOException {
		Assert.assertEquals(0L,
			ByteHelper.readLengthCodedBinary(new byte[] {0}, 0));
		Assert.assertEquals(-1L,
			ByteHelper.readLengthCodedBinary(new byte[] {-5, -1, -7, 4, -7}, 0));
		Assert.assertEquals(65_021L,
			ByteHelper.readLengthCodedBinary(new byte[] {-3, -3, -3, 0, -3}, 0));
		Assert.assertEquals(37_119L,
			ByteHelper.readLengthCodedBinary(new byte[] {-4, -1, -112, 1, -112}, 0));
	}

	@Test
	public void testReadNullTerminatedBytes() {
		Assert.assertArrayEquals(new byte[] {},
			ByteHelper.readNullTerminatedBytes(new byte[] {0}, 0));
		Assert.assertArrayEquals(new byte[] {8},
			ByteHelper.readNullTerminatedBytes(new byte[] {8}, 0));
	}

	@Test
	public void testReadUnsignedIntLittleEndian() {
		Assert.assertEquals(0L,
			ByteHelper.readUnsignedIntLittleEndian(new byte[] {0, 0, 0, 0}, 0));
	}

	@Test
	public void testReadUnsignedMediumLittleEndian() {
		Assert.assertEquals(0,
			ByteHelper.readUnsignedMediumLittleEndian(new byte[] {0, 0, 0}, 0));
	}

	@Test
	public void testReadUnsignedShortLittleEndian() {
		Assert.assertEquals(0,
			ByteHelper.readUnsignedShortLittleEndian(new byte[] {0, 0}, 0));
	}

	@Test
	public void testWrite8ByteUnsignedIntLittleEndian() {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ByteHelper.write8ByteUnsignedIntLittleEndian(72_340_168_547_287_295L, out);

		Assert.assertArrayEquals(new byte[] {-1, -64, 64, 1, 0, 1, 1, 1}, out.toByteArray());
	}

	@Test
	public void testWriteBinaryCodedLengthBytes1() throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ByteHelper.writeBinaryCodedLengthBytes(new byte[] {2, 4}, out);

		Assert.assertArrayEquals(new byte[] {2, 2, 4}, (out.toByteArray()));
	}

	@Test
	public void testWriteBinaryCodedLengthBytes2() throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ByteHelper.writeBinaryCodedLengthBytes(new byte[252], out);

		byte[] expected = new byte[255];
		expected[0] = -4;
		expected[1] = -4;

		Assert.assertArrayEquals(expected, (out.toByteArray()));
	}

	@Test
	public void testWriteBinaryCodedLengthBytes3() throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ByteHelper.writeBinaryCodedLengthBytes(new byte[65536], out);

		byte[] expected = new byte[65540];
		expected[0] = -3;
		expected[3] = 1;

		Assert.assertArrayEquals(expected, (out.toByteArray()));
	}

	@Test
	public void testWriteBinaryCodedLengthBytes4() throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ByteHelper.writeBinaryCodedLengthBytes(new byte[16777216], out);

		byte[] expected = new byte[16777221];
		expected[0] = -2;
		expected[4] = 1;

		Assert.assertArrayEquals(expected, (out.toByteArray()));
	}

	@Test
	public void testWriteFixedLengthBytesFromStart() {
		ByteHelper.writeFixedLengthBytesFromStart(null, 0, null);
	}

	@Test
	public void testWriteFixedLengthBytes() {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ByteHelper.writeFixedLengthBytes(new byte[] {1, -128, 2, 0, 0}, 2, 3, out);

		Assert.assertArrayEquals(new byte[] {2, 0, 0}, out.toByteArray());
	}

	@Test
	public void testWriteNullTerminated() throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ByteHelper.writeNullTerminated(new byte[] {3}, out);

		Assert.assertArrayEquals(new byte[] {3, 0}, out.toByteArray());
	}

	@Test
	public void testWriteNullTerminatedString() throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ByteHelper.writeNullTerminatedString("5", out);

		Assert.assertArrayEquals(new byte[] {53, 0}, out.toByteArray());
	}

	@Test
	public void testWriteUnsignedInt64LittleEndian() {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ByteHelper.writeUnsignedInt64LittleEndian(72_340_168_547_287_295L, out);

		Assert.assertArrayEquals(new byte[] {-1, -64, 64, 1, 0, 1, 1, 1}, out.toByteArray());
	}

	@Test
	public void testWriteUnsignedIntLittleEndian() {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ByteHelper.writeUnsignedIntLittleEndian(50_332_648L, out);

		Assert.assertArrayEquals(new byte[] {-24, 3, 0, 3}, out.toByteArray());
	}

	@Test
	public void testWriteUnsignedMediumLittleEndian() {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ByteHelper.writeUnsignedMediumLittleEndian(131_072, out);

		Assert.assertArrayEquals(new byte[] {0, 0, 2}, out.toByteArray());
	}

	@Test
	public void testWriteUnsignedShortLittleEndian() {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ByteHelper.writeUnsignedShortLittleEndian(1044, out);

		Assert.assertArrayEquals(new byte[] {20, 4}, out.toByteArray());
	}
}
