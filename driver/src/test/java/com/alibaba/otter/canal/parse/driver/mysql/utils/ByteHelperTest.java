package com.alibaba.otter.canal.parse.driver.mysql.utils;

import static org.powermock.api.mockito.PowerMockito.mockStatic;

import com.alibaba.otter.canal.parse.driver.mysql.utils.ByteHelper;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.lang.reflect.Array;

@RunWith(PowerMockRunner.class)
public class ByteHelperTest {

  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Rule public final Timeout globalTimeout = new Timeout(10000);

  /* testedClasses: ByteHelper */
  // Test written by Diffblue Cover.
  @Test
  public void readBinaryCodedLengthBytesInput1ZeroOutput1() throws IOException {

    // Arrange
    final byte[] data = {(byte)-13};
    final int index = 0;

    // Act
    final byte[] actual = ByteHelper.readBinaryCodedLengthBytes(data, index);

    // Assert result
    Assert.assertArrayEquals(new byte[] {(byte)-13}, actual);
  }

  // Test written by Diffblue Cover.
  @PrepareForTest({ByteHelper.class, System.class})
  @Test
  public void readBinaryCodedLengthBytesInput1ZeroOutput12() throws Exception, IOException {

    // Setup mocks
    mockStatic(System.class);

    // Arrange
    final byte[] data = {(byte)-5};
    final int index = 0;

    // Act
    final byte[] actual = ByteHelper.readBinaryCodedLengthBytes(data, index);

    // Assert result
    Assert.assertArrayEquals(new byte[] {(byte)-5}, actual);
  }

  // Test written by Diffblue Cover.
  @PrepareForTest({ByteHelper.class, System.class})
  @Test
  public void readFixedLengthBytesInput0ZeroZeroOutput0() throws Exception {

    // Setup mocks
    mockStatic(System.class);

    // Arrange
    final byte[] data = {};
    final int index = 0;
    final int length = 0;

    // Act
    final byte[] actual = ByteHelper.readFixedLengthBytes(data, index, length);

    // Assert result
    Assert.assertArrayEquals(new byte[] {}, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void readLengthCodedBinaryInput1ZeroOutputZero() throws IOException {

    // Arrange
    final byte[] data = {(byte)0};
    final int index = 0;

    // Act
    final long actual = ByteHelper.readLengthCodedBinary(data, index);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void readLengthCodedBinaryInput6ZeroOutputNegative() throws IOException {

    // Arrange
    final byte[] data = {(byte)-5, (byte)0, (byte)0, (byte)0, (byte)1, (byte)1};
    final int index = 0;

    // Act
    final long actual = ByteHelper.readLengthCodedBinary(data, index);

    // Assert result
    Assert.assertEquals(-1L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void readLengthCodedBinaryInput6ZeroOutputPositive() throws IOException {

    // Arrange
    final byte[] data = {(byte)-4, (byte)4, (byte)4, (byte)6, (byte)5, (byte)1};
    final int index = 0;

    // Act
    final long actual = ByteHelper.readLengthCodedBinary(data, index);

    // Assert result
    Assert.assertEquals(1028L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void readLengthCodedBinaryInput6ZeroOutputZero() throws IOException {

    // Arrange
    final byte[] data = {(byte)-3, (byte)0, (byte)0, (byte)0, (byte)1, (byte)1};
    final int index = 0;

    // Act
    final long actual = ByteHelper.readLengthCodedBinary(data, index);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void readLengthCodedBinaryInput9ZeroOutputZero() throws IOException {

    // Arrange
    final byte[] data = {(byte)-2, (byte)0, (byte)0, (byte)0, (byte)0,
                         (byte)0,  (byte)0, (byte)0, (byte)1};
    final int index = 0;

    // Act
    final long actual = ByteHelper.readLengthCodedBinary(data, index);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void readNullTerminatedBytesInput0ZeroOutput0() {

    // Arrange
    final byte[] data = {};
    final int index = 0;

    // Act
    final byte[] actual = ByteHelper.readNullTerminatedBytes(data, index);

    // Assert result
    Assert.assertArrayEquals(new byte[] {}, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void readNullTerminatedBytesInput1ZeroOutput0() {

    // Arrange
    final byte[] data = {(byte)0};
    final int index = 0;

    // Act
    final byte[] actual = ByteHelper.readNullTerminatedBytes(data, index);

    // Assert result
    Assert.assertArrayEquals(new byte[] {}, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void readNullTerminatedBytesInput1ZeroOutput1() {

    // Arrange
    final byte[] data = {(byte)1};
    final int index = 0;

    // Act
    final byte[] actual = ByteHelper.readNullTerminatedBytes(data, index);

    // Assert result
    Assert.assertArrayEquals(new byte[] {(byte)1}, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void readUnsignedIntLittleEndianInput6ZeroOutputZero() {

    // Arrange
    final byte[] data = {(byte)0, (byte)0, (byte)0, (byte)0, (byte)1, (byte)1};
    final int index = 0;

    // Act
    final long actual = ByteHelper.readUnsignedIntLittleEndian(data, index);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void readUnsignedLongLittleEndianInput14ZeroOutputZero() {

    // Arrange
    final byte[] data = {(byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0,
                         (byte)1, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0};
    final int index = 0;

    // Act
    final long actual = ByteHelper.readUnsignedLongLittleEndian(data, index);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void readUnsignedMediumLittleEndianInput3ZeroOutputZero() {

    // Arrange
    final byte[] data = {(byte)0, (byte)0, (byte)0};
    final int index = 0;

    // Act
    final int actual = ByteHelper.readUnsignedMediumLittleEndian(data, index);

    // Assert result
    Assert.assertEquals(0, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void readUnsignedShortLittleEndianInput2ZeroOutputZero() {

    // Arrange
    final byte[] data = {(byte)0, (byte)0};
    final int index = 0;

    // Act
    final int actual = ByteHelper.readUnsignedShortLittleEndian(data, index);

    // Assert result
    Assert.assertEquals(0, actual);
  }
}
