package com.taobao.tddl.dbsync.binlog;

import static org.mockito.AdditionalMatchers.or;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import com.taobao.tddl.dbsync.binlog.LogBuffer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.BitSet;

@RunWith(PowerMockRunner.class)
public class LogBufferTest {

  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Rule public final Timeout globalTimeout = new Timeout(10000);

  /* testedClasses: LogBuffer */
  // Test written by Diffblue Cover.
  @Test
  public void capacityOutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 0;

    // Act
    final int actual = objectUnderTest.capacity();

    // Assert result
    Assert.assertEquals(0, actual);
  }

  // Test written by Diffblue Cover.

  @Test
  public void constructorInput1PositiveZeroOutputIllegalArgumentException() {

    // Arrange
    final byte[] buffer = {(byte)0};
    final int origin = 1_073_741_825;
    final int limit = 0;

    // Act, creating object to test constructor
    thrown.expect(IllegalArgumentException.class);
    final LogBuffer objectUnderTest = new LogBuffer(buffer, origin, limit);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.

  @Test
  public void constructorInput1PositiveZeroOutputVoid() {

    // Arrange
    final byte[] buffer = {(byte)0};
    final int origin = 1;
    final int limit = 0;

    // Act, creating object to test constructor
    final LogBuffer objectUnderTest = new LogBuffer(buffer, origin, limit);

    // Assert side effects
    Assert.assertEquals(1, objectUnderTest.position);
    Assert.assertArrayEquals(new byte[] {(byte)0}, objectUnderTest.buffer);
    Assert.assertEquals(1, objectUnderTest.origin);
  }

  // Test written by Diffblue Cover.
  @Test
  public void consumeInputNegativeOutputNotNull() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int len = -10;

    // Act
    final LogBuffer actual = objectUnderTest.consume(len);

    // Assert side effects
    Assert.assertEquals(-10, objectUnderTest.position);
    Assert.assertEquals(10, objectUnderTest.limit);
    Assert.assertEquals(-10, objectUnderTest.origin);

    // Assert result
    Assert.assertNotNull(actual);
    Assert.assertEquals(-10, actual.position);
    Assert.assertEquals(0, actual.semival);
    Assert.assertNull(actual.buffer);
    Assert.assertEquals(10, actual.limit);
    Assert.assertEquals(-10, actual.origin);
  }

  // Test written by Diffblue Cover.
  @Test
  public void consumeInputPositiveOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int len = 8;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.consume(len);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void consumeInputZeroOutputNotNull() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int len = 0;

    // Act
    final LogBuffer actual = objectUnderTest.consume(len);

    // Assert result
    Assert.assertNotNull(actual);
    Assert.assertEquals(0, actual.position);
    Assert.assertEquals(0, actual.semival);
    Assert.assertNull(actual.buffer);
    Assert.assertEquals(0, actual.limit);
    Assert.assertEquals(0, actual.origin);
  }

  // Test written by Diffblue Cover.
  @Test
  public void duplicateInputPositiveOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int len = 8_388_612;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.duplicate(len);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void duplicateInputPositivePositiveOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = 1_073_741_823;
    final int len = 1;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.duplicate(pos, len);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void fillBitmapInput0PositiveOutputVoid() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 1;
    final BitSet bitmap = new BitSet();
    final int len = 1;

    // Act
    objectUnderTest.fillBitmap(bitmap, len);

    // Assert side effects
    Assert.assertEquals(1, objectUnderTest.position);
  }

  // Test written by Diffblue Cover.
  @Test
  public void fillBitmapInputNotNullPositiveOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final BitSet bitmap = new BitSet(1);
    final int len = 1;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.fillBitmap(bitmap, len);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void fillBitmapInputNullNegativePositiveOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final BitSet bitmap = null;
    final int pos = -34_463_744;
    final int len = 275_709_945;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.fillBitmap(bitmap, pos, len);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void fillBitmapInputNullPositiveNegativeOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final BitSet bitmap = null;
    final int pos = 794_762;
    final int len = -6_356_999;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.fillBitmap(bitmap, pos, len);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void fillBytesInputNegativeNullZeroZeroOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -35;
    final byte[] dest = null;
    final int destPos = 0;
    final int len = 0;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.fillBytes(pos, dest, destPos, len);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void fillBytesInputNullZeroPositiveOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final byte[] dest = null;
    final int destPos = 0;
    final int len = 1;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.fillBytes(dest, destPos, len);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void fillOutputInputNullNegativePositiveOutputIllegalArgumentException()
      throws IOException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final OutputStream out = null;
    final int pos = -1_224_736_800;
    final int len = 8_984_608;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.fillOutput(out, pos, len);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void fillOutputInputNullPositiveOutputIllegalArgumentException() throws IOException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final OutputStream out = null;
    final int len = 10_000;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.fillOutput(out, len);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void forwardInputNegativeOutputNotNull() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int len = -1_140_850_687;

    // Act
    final LogBuffer actual = objectUnderTest.forward(len);

    // Assert side effects
    Assert.assertEquals(-1_140_850_687, objectUnderTest.position);

    // Assert result
    Assert.assertNotNull(actual);
    Assert.assertEquals(-1_140_850_687, actual.position);
    Assert.assertEquals(0, actual.semival);
    Assert.assertNull(actual.buffer);
    Assert.assertEquals(0, actual.limit);
    Assert.assertEquals(0, actual.origin);
  }

  // Test written by Diffblue Cover.
  @Test
  public void forwardInputPositiveOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int len = 100;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.forward(len);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeInt16InputNegativeOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -1_059_211_413;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeInt16(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeInt16InputPositiveOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = 105;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeInt16(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeInt16InputZeroOutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)0, (byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 2;
    objectUnderTest.origin = 15;
    final int pos = 0;

    // Act
    final int actual = objectUnderTest.getBeInt16(pos);

    // Assert result
    Assert.assertEquals(0, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeInt16OutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeInt16();

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeInt16OutputNegative() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 17;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)-2, (byte)-2, (byte)-2, (byte)-2, (byte)-2,
                                (byte)-2, (byte)-2, (byte)-2, (byte)-2, (byte)-2,
                                (byte)-2, (byte)-2, (byte)-2, (byte)-2, (byte)-2,
                                (byte)-2, (byte)-2, (byte)-1, (byte)-1, (byte)-2};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 1_073_741_827;

    // Act
    final int actual = objectUnderTest.getBeInt16();

    // Assert side effects
    Assert.assertEquals(19, objectUnderTest.position);

    // Assert result
    Assert.assertEquals(-1, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeInt24InputNegativeOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -1_215_752_192;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeInt24(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeInt24InputPositiveOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = 1_073_741_822;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeInt24(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeInt24InputZeroOutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {
        (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
        (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)0, (byte)0, (byte)0, (byte)1, (byte)1,
        (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 3;
    objectUnderTest.origin = 15;
    final int pos = 0;

    // Act
    final int actual = objectUnderTest.getBeInt24(pos);

    // Assert result
    Assert.assertEquals(0, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeInt24OutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeInt24();

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeInt24OutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 15;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)0, (byte)0, (byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 18;

    // Act
    final int actual = objectUnderTest.getBeInt24();

    // Assert side effects
    Assert.assertEquals(18, objectUnderTest.position);

    // Assert result
    Assert.assertEquals(0, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeInt32InputNegativeOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -1_249_306_624;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeInt32(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeInt32InputPositiveOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = 1_073_741_821;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeInt32(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeInt32InputZeroOutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)0, (byte)0, (byte)0, (byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 6;
    objectUnderTest.origin = 15;
    final int pos = 0;

    // Act
    final int actual = objectUnderTest.getBeInt32(pos);

    // Assert result
    Assert.assertEquals(0, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeInt32OutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeInt32();

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeInt32OutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 14;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)0, (byte)0, (byte)0, (byte)0, (byte)1};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 18;

    // Act
    final int actual = objectUnderTest.getBeInt32();

    // Assert side effects
    Assert.assertEquals(18, objectUnderTest.position);

    // Assert result
    Assert.assertEquals(0, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeLong48InputNegativeOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -1_207_959_552;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeLong48(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeLong48InputPositiveOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = 1_002_438_651;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeLong48(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeLong48InputZeroOutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)0, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 6;
    objectUnderTest.origin = 15;
    final int pos = 0;

    // Act
    final long actual = objectUnderTest.getBeLong48(pos);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeLong48OutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeLong48();

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeLong48OutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 13;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0,
                                (byte)0, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 51;

    // Act
    final long actual = objectUnderTest.getBeLong48();

    // Assert side effects
    Assert.assertEquals(19, objectUnderTest.position);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeLong64InputNegativeOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -4096;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeLong64(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeLong64InputPositiveOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = 1_073_741_817;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeLong64(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeLong64InputZeroOutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)0, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0,
                                (byte)0, (byte)1, (byte)1, (byte)1};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 14;
    objectUnderTest.origin = 14;
    final int pos = 0;

    // Act
    final long actual = objectUnderTest.getBeLong64(pos);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeLong64OutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeLong64();

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeLong64OutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 10;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)0, (byte)0, (byte)0, (byte)0,
                                (byte)0, (byte)0, (byte)0, (byte)0, (byte)1};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 18;

    // Act
    final long actual = objectUnderTest.getBeLong64();

    // Assert side effects
    Assert.assertEquals(18, objectUnderTest.position);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeUint16InputNegativeOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -1_059_211_413;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeUint16(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeUint16InputPositiveOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = 105;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeUint16(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeUint16InputZeroOutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)0, (byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 2;
    objectUnderTest.origin = 15;
    final int pos = 0;

    // Act
    final int actual = objectUnderTest.getBeUint16(pos);

    // Assert result
    Assert.assertEquals(0, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeUint16OutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeUint16();

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeUint16OutputPositive() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 17;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0,
                                (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0,
                                (byte)0, (byte)0, (byte)0, (byte)1, (byte)1};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 1_073_741_827;

    // Act
    final int actual = objectUnderTest.getBeUint16();

    // Assert side effects
    Assert.assertEquals(19, objectUnderTest.position);

    // Assert result
    Assert.assertEquals(257, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeUint24InputNegativeOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -1_215_752_192;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeUint24(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeUint24InputPositiveOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = 1_073_741_822;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeUint24(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeUint24InputZeroOutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {
        (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
        (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)0, (byte)0, (byte)0, (byte)1, (byte)1,
        (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 3;
    objectUnderTest.origin = 15;
    final int pos = 0;

    // Act
    final int actual = objectUnderTest.getBeUint24(pos);

    // Assert result
    Assert.assertEquals(0, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeUint24OutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeUint24();

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeUint24OutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 15;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)0, (byte)0, (byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 18;

    // Act
    final int actual = objectUnderTest.getBeUint24();

    // Assert side effects
    Assert.assertEquals(18, objectUnderTest.position);

    // Assert result
    Assert.assertEquals(0, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeUint32InputNegativeOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -1_249_306_624;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeUint32(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeUint32InputPositiveOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = 1_073_741_821;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeUint32(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeUint32InputZeroOutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)0, (byte)0, (byte)0, (byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 6;
    objectUnderTest.origin = 15;
    final int pos = 0;

    // Act
    final long actual = objectUnderTest.getBeUint32(pos);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeUint32OutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeUint32();

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeUint32OutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 14;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)0, (byte)0, (byte)0, (byte)0, (byte)1};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 18;

    // Act
    final long actual = objectUnderTest.getBeUint32();

    // Assert side effects
    Assert.assertEquals(18, objectUnderTest.position);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeUlong40InputNegativeOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -1_000_000_000;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeUlong40(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeUlong40InputPositiveOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = 1_610_612_733;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeUlong40(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeUlong40InputZeroOutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)0, (byte)0, (byte)0,
                                (byte)0, (byte)0, (byte)1, (byte)1, (byte)1, (byte)1};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 5;
    objectUnderTest.origin = 15;
    final int pos = 0;

    // Act
    final long actual = objectUnderTest.getBeUlong40(pos);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeUlong40OutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeUlong40();

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeUlong40OutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 14;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)0, (byte)0, (byte)0, (byte)0,
                                (byte)0, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 51;

    // Act
    final long actual = objectUnderTest.getBeUlong40();

    // Assert side effects
    Assert.assertEquals(19, objectUnderTest.position);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeUlong48InputNegativeOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -1_207_959_552;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeUlong48(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeUlong48InputPositiveOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = 1_002_438_651;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeUlong48(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeUlong48InputZeroOutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)0, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 6;
    objectUnderTest.origin = 15;
    final int pos = 0;

    // Act
    final long actual = objectUnderTest.getBeUlong48(pos);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeUlong48OutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeUlong48();

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeUlong48OutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 13;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0,
                                (byte)0, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 51;

    // Act
    final long actual = objectUnderTest.getBeUlong48();

    // Assert side effects
    Assert.assertEquals(19, objectUnderTest.position);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeUlong56InputNegativeOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -1_207_959_552;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeUlong56(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeUlong56InputPositiveOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = 123;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeUlong56(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeUlong56InputZeroOutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)0,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)0, (byte)0, (byte)0,
                                (byte)0, (byte)0, (byte)0, (byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 7;
    objectUnderTest.origin = 15;
    final int pos = 0;

    // Act
    final long actual = objectUnderTest.getBeUlong56(pos);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeUlong56OutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeUlong56();

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeUlong56OutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 11;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)0, (byte)0, (byte)0,
                                (byte)0, (byte)0, (byte)0, (byte)0, (byte)1};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 18;

    // Act
    final long actual = objectUnderTest.getBeUlong56();

    // Assert side effects
    Assert.assertEquals(18, objectUnderTest.position);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeUlong64InputNegativeOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -1_215_752_192;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeUlong64(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBeUlong64OutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBeUlong64();

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBitmapInputNegativePositiveOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -159_374_617;
    final int len = 1_274_996_856;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getBitmap(pos, len);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBitmapInputPositiveOutput2() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)2};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 1;
    final int len = 1;

    // Act
    final BitSet actual = objectUnderTest.getBitmap(len);

    // Assert side effects
    Assert.assertEquals(1, objectUnderTest.position);

    // Assert result
    final BitSet bitSet = new BitSet();
    bitSet.set(1);
    Assert.assertEquals(bitSet, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBitmapInputPositiveOutput3() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)4};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 1;
    final int len = 1;

    // Act
    final BitSet actual = objectUnderTest.getBitmap(len);

    // Assert side effects
    Assert.assertEquals(1, objectUnderTest.position);

    // Assert result
    final BitSet bitSet = new BitSet();
    bitSet.set(2);
    Assert.assertEquals(bitSet, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBitmapInputPositiveOutput5() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)8};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 1;
    final int len = 1;

    // Act
    final BitSet actual = objectUnderTest.getBitmap(len);

    // Assert side effects
    Assert.assertEquals(1, objectUnderTest.position);

    // Assert result
    final BitSet bitSet = new BitSet();
    bitSet.set(3);
    Assert.assertEquals(bitSet, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBitmapInputPositiveOutput6() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)16};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 1;
    final int len = 1;

    // Act
    final BitSet actual = objectUnderTest.getBitmap(len);

    // Assert side effects
    Assert.assertEquals(1, objectUnderTest.position);

    // Assert result
    final BitSet bitSet = new BitSet();
    bitSet.set(4);
    Assert.assertEquals(bitSet, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBitmapInputPositiveOutput7() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)64};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 1;
    final int len = 1;

    // Act
    final BitSet actual = objectUnderTest.getBitmap(len);

    // Assert side effects
    Assert.assertEquals(1, objectUnderTest.position);

    // Assert result
    final BitSet bitSet = new BitSet();
    bitSet.set(6);
    Assert.assertEquals(bitSet, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBitmapInputZeroOutput1() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int len = 0;

    // Act
    final BitSet actual = objectUnderTest.getBitmap(len);

    // Assert result
    final BitSet bitSet = new BitSet();
    Assert.assertEquals(bitSet, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBitmapInputZeroPositiveOutput2() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)2};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 1;
    objectUnderTest.origin = 0;
    final int pos = 0;
    final int len = 1;

    // Act
    final BitSet actual = objectUnderTest.getBitmap(pos, len);

    // Assert result
    final BitSet bitSet = new BitSet();
    bitSet.set(1);
    Assert.assertEquals(bitSet, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBitmapInputZeroPositiveOutput5() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)8};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 1;
    objectUnderTest.origin = 0;
    final int pos = 0;
    final int len = 1;

    // Act
    final BitSet actual = objectUnderTest.getBitmap(pos, len);

    // Assert result
    final BitSet bitSet = new BitSet();
    bitSet.set(3);
    Assert.assertEquals(bitSet, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBitmapInputZeroPositiveOutput6() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)16};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 1;
    objectUnderTest.origin = 0;
    final int pos = 0;
    final int len = 1;

    // Act
    final BitSet actual = objectUnderTest.getBitmap(pos, len);

    // Assert result
    final BitSet bitSet = new BitSet();
    bitSet.set(4);
    Assert.assertEquals(bitSet, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBitmapInputZeroPositiveOutput7() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)64};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 1;
    objectUnderTest.origin = 0;
    final int pos = 0;
    final int len = 1;

    // Act
    final BitSet actual = objectUnderTest.getBitmap(pos, len);

    // Assert result
    final BitSet bitSet = new BitSet();
    bitSet.set(6);
    Assert.assertEquals(bitSet, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getBitmapInputZeroZeroOutput1() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = 0;
    final int len = 0;

    // Act
    final BitSet actual = objectUnderTest.getBitmap(pos, len);

    // Assert result
    final BitSet bitSet = new BitSet();
    Assert.assertEquals(bitSet, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getDataInputNegativeZeroOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -1_215_752_192;
    final int len = 0;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getData(pos, len);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @PrepareForTest({LogBuffer.class, System.class})
  @Test
  public void getDataInputZeroZeroOutput0() throws Exception {

    // Setup mocks
    mockStatic(System.class);

    // Arrange
    final byte[] myByteArray = {};
    final LogBuffer objectUnderTest = new LogBuffer(myByteArray, 0, 0);
    final int pos = 0;
    final int len = 0;

    // Act
    final byte[] actual = objectUnderTest.getData(pos, len);

    // Assert result
    Assert.assertArrayEquals(new byte[] {}, actual);
  }

  // Test written by Diffblue Cover.
  @PrepareForTest({LogBuffer.class, System.class})
  @Test
  public void getDataOutput0() throws Exception {

    // Setup mocks
    mockStatic(System.class);

    // Arrange
    final byte[] myByteArray = {};
    final LogBuffer objectUnderTest = new LogBuffer(myByteArray, 0, 0);

    // Act
    final byte[] actual = objectUnderTest.getData();

    // Assert result
    Assert.assertArrayEquals(new byte[] {}, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getDouble64InputNegativeOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -1_215_752_192;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getDouble64(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getDouble64InputZeroOutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)0, (byte)0, (byte)0, (byte)0,
                                (byte)0, (byte)0, (byte)0, (byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 14;
    objectUnderTest.origin = 14;
    final int pos = 0;

    // Act
    final double actual = objectUnderTest.getDouble64(pos);

    // Assert result
    Assert.assertEquals(0.0, actual, 0.0);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getDouble64OutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getDouble64();

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getDouble64OutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 10;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)0, (byte)0, (byte)0, (byte)0,
                                (byte)0, (byte)0, (byte)0, (byte)0, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 18;

    // Act
    final double actual = objectUnderTest.getDouble64();

    // Assert side effects
    Assert.assertEquals(18, objectUnderTest.position);

    // Assert result
    Assert.assertEquals(0.0, actual, 0.0);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getFixStringInputNegativePositiveNotNullOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -569_761_897;
    final int len = 267_772_025;
    final String charsetName = "3";

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getFixString(pos, len, charsetName);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getFixStringInputNegativeZeroOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -21;
    final int len = 0;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getFixString(pos, len);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getFixStringInputPositiveNotNullOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int len = 1;
    final String charsetName = "3";

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getFixString(len, charsetName);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getFixStringInputPositiveNotNullOutputIllegalArgumentException2()
      throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 8;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)0, (byte)0, (byte)0, (byte)0, (byte)0,
                                (byte)0, (byte)0, (byte)0, (byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 1_065_353_216;
    final int len = 536_870_944;
    final String charsetName = "foo";

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getFixString(len, charsetName);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getFixStringInputPositiveNotNullOutputIllegalArgumentException3()
      throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 15;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0,
                                (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0,
                                (byte)0, (byte)0, (byte)0, (byte)1, (byte)0, (byte)0,
                                (byte)0, (byte)0, (byte)0, (byte)0, (byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 16;
    final int len = 1;
    final String charsetName = "foo";

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getFixString(len, charsetName);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getFixStringInputPositiveOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int len = 1;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getFixString(len);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getFixStringInputPositivePositiveNotNullOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = 1_040_850_839;
    final int len = 537_256_057;
    final String charsetName = "3";

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getFixString(pos, len, charsetName);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getFixStringInputZeroNotNullOutputIllegalArgumentException()
      throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 0;
    final int len = 0;
    final String charsetName = "foo";

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getFixString(len, charsetName);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getFixStringInputZeroNotNullOutputStringIndexOutOfBoundsException()
      throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = -21;
    objectUnderTest.semival = 0;
    objectUnderTest.buffer = null;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 0;
    final int len = 0;
    final String charsetName = "foo";

    // Act
    thrown.expect(StringIndexOutOfBoundsException.class);
    objectUnderTest.getFixString(len, charsetName);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getFixStringInputZeroPositiveNotNullOutputIllegalArgumentException()
      throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {
        (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0,
        (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0,
        (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 1;
    objectUnderTest.origin = 28;
    final int pos = 0;
    final int len = 1;
    final String charsetName = "foo";

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getFixString(pos, len, charsetName);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getFixStringInputZeroPositiveNotNullOutputIllegalArgumentException2()
      throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {
        (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0,
        (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0,
        (byte)0, (byte)0, (byte)1, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 1;
    objectUnderTest.origin = 22;
    final int pos = 0;
    final int len = 1;
    final String charsetName = "foo";

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getFixString(pos, len, charsetName);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getFixStringInputZeroZeroNotNullOutputIllegalArgumentException()
      throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 0;
    final int pos = 0;
    final int len = 0;
    final String charsetName = "foo";

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getFixString(pos, len, charsetName);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getFixStringInputZeroZeroNotNullOutputStringIndexOutOfBoundsException()
      throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    objectUnderTest.buffer = null;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = -4096;
    final int pos = 0;
    final int len = 0;
    final String charsetName = "foo";

    // Act
    thrown.expect(StringIndexOutOfBoundsException.class);
    objectUnderTest.getFixString(pos, len, charsetName);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getFloat32InputNegativeOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -1_215_752_192;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getFloat32(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getFloat32OutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getFloat32();

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getFloat32OutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 14;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)0, (byte)0, (byte)0, (byte)0, (byte)1};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 18;

    // Act
    final float actual = objectUnderTest.getFloat32();

    // Assert side effects
    Assert.assertEquals(18, objectUnderTest.position);

    // Assert result
    Assert.assertEquals(0.0f, actual, 0.0f);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getFullStringInputNegativeNegativeNotNullOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -2_147_483_648;
    final int len = -2_147_483_647;
    final String charsetName = "foo";

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getFullString(pos, len, charsetName);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getFullStringInputNegativeNegativeNotNullOutputIllegalArgumentException2() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -2_147_483_648;
    final int len = -2_147_483_648;
    final String charsetName = "foo";

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getFullString(pos, len, charsetName);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getFullStringInputNegativeNotNullOutputStringIndexOutOfBoundsException()
      throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    objectUnderTest.buffer = null;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 0;
    final int len = -21;
    final String charsetName = "foo";

    // Act
    thrown.expect(StringIndexOutOfBoundsException.class);
    objectUnderTest.getFullString(len, charsetName);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getFullStringInputPositiveZeroNotNullOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = 1_076_887_585;
    final int len = 0;
    final String charsetName = "foo";

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getFullString(pos, len, charsetName);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getFullStringInputZeroNegativeNotNullOutputStringIndexOutOfBoundsException()
      throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    objectUnderTest.buffer = null;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 0;
    final int pos = 0;
    final int len = -2_130_706_432;
    final String charsetName = "foo";

    // Act
    thrown.expect(StringIndexOutOfBoundsException.class);
    objectUnderTest.getFullString(pos, len, charsetName);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getFullStringInputZeroNotNullOutputIllegalArgumentException()
      throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = -1_215_754_240;
    objectUnderTest.semival = 0;
    objectUnderTest.buffer = null;
    objectUnderTest.limit = -1_222_639_616;
    objectUnderTest.origin = -2048;
    final int len = 0;
    final String charsetName = "foo";

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getFullString(len, charsetName);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getFullStringInputZeroNotNullOutputIllegalArgumentException2()
      throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 0;
    final int len = 0;
    final String charsetName = "foo";

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getFullString(len, charsetName);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getFullStringInputZeroZeroNotNullOutputIllegalArgumentException()
      throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 0;
    final int pos = 0;
    final int len = 0;
    final String charsetName = "foo";

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getFullString(pos, len, charsetName);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getInt16InputNegativeOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -1_059_211_413;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getInt16(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getInt16InputPositiveOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = 1_610_612_735;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getInt16(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getInt16InputZeroOutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)0, (byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 2;
    objectUnderTest.origin = 15;
    final int pos = 0;

    // Act
    final int actual = objectUnderTest.getInt16(pos);

    // Assert result
    Assert.assertEquals(0, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getInt16OutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getInt16();

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getInt16OutputPositive() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 17;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0,
                                (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0,
                                (byte)0, (byte)0, (byte)0, (byte)1, (byte)1, (byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 1_073_741_827;

    // Act
    final int actual = objectUnderTest.getInt16();

    // Assert side effects
    Assert.assertEquals(19, objectUnderTest.position);

    // Assert result
    Assert.assertEquals(257, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getInt24InputNegativeOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -1_215_752_192;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getInt24(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getInt24InputPositiveOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = 1_073_741_822;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getInt24(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getInt24InputZeroOutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {
        (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
        (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
        (byte)1, (byte)1, (byte)1, (byte)0, (byte)0, (byte)0, (byte)1, (byte)1, (byte)1};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 3;
    objectUnderTest.origin = 23;
    final int pos = 0;

    // Act
    final int actual = objectUnderTest.getInt24(pos);

    // Assert result
    Assert.assertEquals(0, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getInt24OutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getInt24();

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getInt24OutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 15;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)0, (byte)0, (byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 18;

    // Act
    final int actual = objectUnderTest.getInt24();

    // Assert side effects
    Assert.assertEquals(18, objectUnderTest.position);

    // Assert result
    Assert.assertEquals(0, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getInt32InputNegativeOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -1_215_752_192;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getInt32(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getInt32InputPositiveOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = 1_073_741_821;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getInt32(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getInt32InputZeroOutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {
        (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
        (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
        (byte)1, (byte)1, (byte)1, (byte)0, (byte)0, (byte)0, (byte)0, (byte)1, (byte)1};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 6;
    objectUnderTest.origin = 23;
    final int pos = 0;

    // Act
    final int actual = objectUnderTest.getInt32(pos);

    // Assert result
    Assert.assertEquals(0, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getInt32OutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getInt32();

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getInt32OutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 14;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)0, (byte)0, (byte)0, (byte)0, (byte)1};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 18;

    // Act
    final int actual = objectUnderTest.getInt32();

    // Assert side effects
    Assert.assertEquals(18, objectUnderTest.position);

    // Assert result
    Assert.assertEquals(0, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getInt8InputNegativeOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -10;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getInt8(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getInt8InputZeroOutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {
        (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
        (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
        (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 1;
    objectUnderTest.origin = 28;
    final int pos = 0;

    // Act
    final int actual = objectUnderTest.getInt8(pos);

    // Assert result
    Assert.assertEquals(0, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getInt8OutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getInt8();

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getInt8OutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 29;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {
        (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
        (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
        (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 93;

    // Act
    final int actual = objectUnderTest.getInt8();

    // Assert side effects
    Assert.assertEquals(30, objectUnderTest.position);

    // Assert result
    Assert.assertEquals(0, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getLong48InputNegativeOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -1_207_959_552;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getLong48(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getLong48InputPositiveOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = 107;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getLong48(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getLong48InputZeroOutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 6;
    objectUnderTest.origin = 15;
    final int pos = 0;

    // Act
    final long actual = objectUnderTest.getLong48(pos);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getLong48OutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getLong48();

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getLong48OutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 13;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0,
                                (byte)0, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 51;

    // Act
    final long actual = objectUnderTest.getLong48();

    // Assert side effects
    Assert.assertEquals(19, objectUnderTest.position);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getLong64InputNegativeOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -1_215_752_192;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getLong64(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getLong64InputPositiveOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = 1_073_741_817;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getLong64(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getLong64InputZeroOutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0,
                                (byte)0, (byte)1, (byte)1, (byte)1};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 14;
    objectUnderTest.origin = 14;
    final int pos = 0;

    // Act
    final long actual = objectUnderTest.getLong64(pos);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getLong64OutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getLong64();

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getLong64OutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 10;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)0, (byte)0,
                                (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 18;

    // Act
    final long actual = objectUnderTest.getLong64();

    // Assert side effects
    Assert.assertEquals(18, objectUnderTest.position);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getPackedLongInputNegativeOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -65;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getPackedLong(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getPackedLongInputPositiveOutputPositive() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {
        (byte)-114, (byte)65, (byte)77, (byte)77, (byte)77, (byte)77, (byte)77, (byte)77, (byte)77,
        (byte)77,   (byte)77, (byte)77, (byte)77, (byte)-2, (byte)76, (byte)76, (byte)76, (byte)76,
        (byte)109,  (byte)77, (byte)77, (byte)77, (byte)77, (byte)77, (byte)77};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 134_217_728;
    objectUnderTest.origin = -5;
    final int pos = 18;

    // Act
    final long actual = objectUnderTest.getPackedLong(pos);

    // Assert result
    Assert.assertEquals(1_280_068_684L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getPackedLongInputPositiveOutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1,  (byte)1, (byte)-4, (byte)1, (byte)-4, (byte)1, (byte)1,
                                (byte)-4, (byte)0, (byte)0,  (byte)1, (byte)1,  (byte)1, (byte)1,
                                (byte)1,  (byte)1, (byte)1,  (byte)1, (byte)1,  (byte)1, (byte)1,
                                (byte)0,  (byte)0, (byte)1,  (byte)1, (byte)1,  (byte)1};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 1_610_612_757;
    objectUnderTest.origin = -1_610_612_744;
    final int pos = 1_610_612_751;

    // Act
    final long actual = objectUnderTest.getPackedLong(pos);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getPackedLongInputPositiveOutputZero2() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1,  (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1,  (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)-3, (byte)0, (byte)0, (byte)0, (byte)1};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 32_774;
    objectUnderTest.origin = -32_754;
    final int pos = 32_768;

    // Act
    final long actual = objectUnderTest.getPackedLong(pos);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getPackedLongInputZeroOutputNegative() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)-6, (byte)-6, (byte)-6, (byte)-6, (byte)-6, (byte)-6,
                                (byte)-6, (byte)-6, (byte)-6, (byte)-6, (byte)-6, (byte)-6,
                                (byte)-6, (byte)-6, (byte)-6, (byte)-6, (byte)-6, (byte)-6,
                                (byte)-6, (byte)-6, (byte)-6, (byte)-6, (byte)-6, (byte)-6,
                                (byte)-6, (byte)-6, (byte)-6, (byte)-6, (byte)-5, (byte)-6};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 1;
    objectUnderTest.origin = 28;
    final int pos = 0;

    // Act
    final long actual = objectUnderTest.getPackedLong(pos);

    // Assert result
    Assert.assertEquals(-1L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getPackedLongInputZeroOutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {
        (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
        (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
        (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)0, (byte)1};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 1;
    objectUnderTest.origin = 28;
    final int pos = 0;

    // Act
    final long actual = objectUnderTest.getPackedLong(pos);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getPackedLongOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getPackedLong();

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getPackedLongOutputIllegalArgumentException2() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 1;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)-3, (byte)-4, (byte)-3};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = -1_215_752_192;
    objectUnderTest.origin = 1_215_752_195;
    try {

      // Act
      thrown.expect(IllegalArgumentException.class);
      objectUnderTest.getPackedLong();
    } catch (IllegalArgumentException ex) {

      // Assert side effects
      Assert.assertEquals(2, objectUnderTest.position);
      throw ex;
    }
  }

  // Test written by Diffblue Cover.
  @Test
  public void getPackedLongOutputIllegalArgumentException3() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 1;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)-2, (byte)-1, (byte)-2};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = -1_215_752_192;
    objectUnderTest.origin = 1_215_752_197;
    try {

      // Act
      thrown.expect(IllegalArgumentException.class);
      objectUnderTest.getPackedLong();
    } catch (IllegalArgumentException ex) {

      // Assert side effects
      Assert.assertEquals(2, objectUnderTest.position);
      throw ex;
    }
  }

  // Test written by Diffblue Cover.
  @Test
  public void getPackedLongOutputIllegalArgumentException4() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 1;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)-4, (byte)-3, (byte)-4};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = -1_215_752_192;
    objectUnderTest.origin = 1_215_752_196;
    try {

      // Act
      thrown.expect(IllegalArgumentException.class);
      objectUnderTest.getPackedLong();
    } catch (IllegalArgumentException ex) {

      // Assert side effects
      Assert.assertEquals(2, objectUnderTest.position);
      throw ex;
    }
  }

  // Test written by Diffblue Cover.
  @Test
  public void getPackedLongOutputNegative() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 29;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)-6, (byte)-6, (byte)-6, (byte)-6, (byte)-6, (byte)-6,
                                (byte)-6, (byte)-6, (byte)-6, (byte)-6, (byte)-6, (byte)-6,
                                (byte)-6, (byte)-6, (byte)-6, (byte)-6, (byte)-6, (byte)-6,
                                (byte)-6, (byte)-6, (byte)-6, (byte)-6, (byte)-6, (byte)-6,
                                (byte)-6, (byte)-6, (byte)-6, (byte)-6, (byte)-6, (byte)-5};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 31;

    // Act
    final long actual = objectUnderTest.getPackedLong();

    // Assert side effects
    Assert.assertEquals(30, objectUnderTest.position);

    // Assert result
    Assert.assertEquals(-1L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getPackedLongOutputPositive() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 5;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)126, (byte)126, (byte)126, (byte)127, (byte)127, (byte)-1,
                                (byte)126, (byte)126, (byte)126, (byte)126, (byte)127, (byte)127,
                                (byte)127, (byte)127, (byte)127, (byte)127};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 33_554_437;

    // Act
    final long actual = objectUnderTest.getPackedLong();

    // Assert side effects
    Assert.assertEquals(14, objectUnderTest.position);

    // Assert result
    Assert.assertEquals(2_122_219_134L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getPackedLongOutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 29;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {
        (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
        (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
        (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 31;

    // Act
    final long actual = objectUnderTest.getPackedLong();

    // Assert side effects
    Assert.assertEquals(30, objectUnderTest.position);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getPackedLongOutputZero2() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 16;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1,  (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1,  (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)-4, (byte)0, (byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 1_073_741_827;

    // Act
    final long actual = objectUnderTest.getPackedLong();

    // Assert side effects
    Assert.assertEquals(19, objectUnderTest.position);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getPackedLongOutputZero3() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 14;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1,  (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1,  (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)-3, (byte)0, (byte)0, (byte)0, (byte)1, (byte)1, (byte)1};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 18;

    // Act
    final long actual = objectUnderTest.getPackedLong();

    // Assert side effects
    Assert.assertEquals(18, objectUnderTest.position);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getStringInputNegativeNotNullOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -1_073_741_760;
    final String charsetName = "1";

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getString(pos, charsetName);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getStringInputNegativeOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -22;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getString(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getStringInputNotNullOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final String charsetName = "2";

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getString(charsetName);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getStringInputNotNullOutputIllegalArgumentException2()
      throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 29;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)-29, (byte)-29, (byte)-29, (byte)-29, (byte)-29, (byte)-29,
                                (byte)-29, (byte)-29, (byte)-29, (byte)-29, (byte)-29, (byte)-29,
                                (byte)-29, (byte)-29, (byte)-29, (byte)-29, (byte)-29, (byte)-29,
                                (byte)-29, (byte)-29, (byte)-29, (byte)-29, (byte)-29, (byte)-29,
                                (byte)-29, (byte)-29, (byte)-29, (byte)-29, (byte)-29, (byte)-30};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 12;
    objectUnderTest.origin = 144;
    final String charsetName = "foo";

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getString(charsetName);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getStringInputNotNullOutputIllegalArgumentException3()
      throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 1;
    final String charsetName = "foo";

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getString(charsetName);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getStringInputNotNullOutputStringIndexOutOfBoundsException()
      throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)107};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 1_572_973;
    final String charsetName = "foo";

    // Act
    thrown.expect(StringIndexOutOfBoundsException.class);
    objectUnderTest.getString(charsetName);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getStringInputPositiveNotNullOutputIllegalArgumentException()
      throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)-2, (byte)-2, (byte)-2, (byte)-2, (byte)-2, (byte)-2,
                                (byte)-2, (byte)-2, (byte)-2, (byte)-2, (byte)-2, (byte)-2,
                                (byte)-2, (byte)-2, (byte)-2, (byte)-2, (byte)-1};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 1_073_741_569;
    objectUnderTest.origin = -1_073_741_552;
    final int pos = 1_073_741_568;
    final String charsetName = "foo";

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getString(pos, charsetName);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getStringInputPositiveNotNullOutputStringIndexOutOfBoundsException()
      throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)6, (byte)6, (byte)7, (byte)6, (byte)6};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 1_614_807_049;
    objectUnderTest.origin = -1_610_612_736;
    final int pos = 1_610_612_738;
    final String charsetName = "foo";

    // Act
    thrown.expect(StringIndexOutOfBoundsException.class);
    objectUnderTest.getString(pos, charsetName);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getStringInputZeroNotNullOutputIllegalArgumentException()
      throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 1;
    objectUnderTest.origin = 0;
    final int pos = 0;
    final String charsetName = "foo";

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getString(pos, charsetName);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getStringInputZeroOutputNotNull() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)3, (byte)102, (byte)111, (byte)111, (byte)2, (byte)2};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 4;
    objectUnderTest.origin = 0;
    final int pos = 0;

    // Act
    final String actual = objectUnderTest.getString(pos);

    // Assert result
    Assert.assertEquals("foo", actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getStringOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getString();

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getStringOutputNotNull() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 3;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)3,   (byte)102, (byte)111, (byte)3,
                                (byte)102, (byte)111, (byte)111};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 7;

    // Act
    final String actual = objectUnderTest.getString();

    // Assert side effects
    Assert.assertEquals(7, objectUnderTest.position);

    // Assert result
    Assert.assertEquals("foo", actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUint16InputNegativeOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -1_215_752_192;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getUint16(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUint16InputPositiveOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = 1_073_741_823;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getUint16(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUint16InputZeroOutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)0, (byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 2;
    objectUnderTest.origin = 15;
    final int pos = 0;

    // Act
    final int actual = objectUnderTest.getUint16(pos);

    // Assert result
    Assert.assertEquals(0, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUint16OutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getUint16();

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUint16OutputPositive() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 17;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)3, (byte)3, (byte)3, (byte)3, (byte)3, (byte)3,
                                (byte)3, (byte)3, (byte)3, (byte)3, (byte)3, (byte)3,
                                (byte)3, (byte)3, (byte)3, (byte)3, (byte)3, (byte)2,
                                (byte)2, (byte)3, (byte)3, (byte)3, (byte)3, (byte)3};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 1_073_741_827;

    // Act
    final int actual = objectUnderTest.getUint16();

    // Assert side effects
    Assert.assertEquals(19, objectUnderTest.position);

    // Assert result
    Assert.assertEquals(514, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUint24InputNegativeOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -1_215_752_192;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getUint24(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUint24InputPositiveOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = 1_073_741_822;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getUint24(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUint24InputZeroOutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {
        (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
        (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
        (byte)1, (byte)1, (byte)1, (byte)0, (byte)0, (byte)0, (byte)1, (byte)1, (byte)1};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 3;
    objectUnderTest.origin = 23;
    final int pos = 0;

    // Act
    final int actual = objectUnderTest.getUint24(pos);

    // Assert result
    Assert.assertEquals(0, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUint24OutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getUint24();

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUint24OutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 15;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)0, (byte)0, (byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 18;

    // Act
    final int actual = objectUnderTest.getUint24();

    // Assert side effects
    Assert.assertEquals(18, objectUnderTest.position);

    // Assert result
    Assert.assertEquals(0, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUint32InputNegativeOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -1_215_752_192;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getUint32(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUint32InputPositiveOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = 1_073_741_821;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getUint32(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUint32InputZeroOutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)0, (byte)0, (byte)0, (byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 6;
    objectUnderTest.origin = 15;
    final int pos = 0;

    // Act
    final long actual = objectUnderTest.getUint32(pos);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUint32OutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getUint32();

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUint32OutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 14;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)0, (byte)0, (byte)0, (byte)0, (byte)1};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 18;

    // Act
    final long actual = objectUnderTest.getUint32();

    // Assert side effects
    Assert.assertEquals(18, objectUnderTest.position);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUint8InputNegativeOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -1_215_752_192;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getUint8(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUint8InputZeroOutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {
        (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
        (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
        (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 1;
    objectUnderTest.origin = 28;
    final int pos = 0;

    // Act
    final int actual = objectUnderTest.getUint8(pos);

    // Assert result
    Assert.assertEquals(0, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUint8OutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getUint8();

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUint8OutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 29;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {
        (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
        (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
        (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 93;

    // Act
    final int actual = objectUnderTest.getUint8();

    // Assert side effects
    Assert.assertEquals(30, objectUnderTest.position);

    // Assert result
    Assert.assertEquals(0, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUlong40InputNegativeOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -1_073_741_823;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getUlong40(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUlong40InputPositiveOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = 2_080_374_780;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getUlong40(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUlong40InputZeroOutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)0, (byte)0, (byte)0,
                                (byte)0, (byte)0, (byte)1, (byte)1};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 5;
    objectUnderTest.origin = 15;
    final int pos = 0;

    // Act
    final long actual = objectUnderTest.getUlong40(pos);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUlong40OutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getUlong40();

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUlong40OutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 14;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)0, (byte)0, (byte)0, (byte)0,
                                (byte)0, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 51;

    // Act
    final long actual = objectUnderTest.getUlong40();

    // Assert side effects
    Assert.assertEquals(19, objectUnderTest.position);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUlong48InputNegativeOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -1_207_959_552;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getUlong48(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUlong48InputPositiveOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = 2_097_150;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getUlong48(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUlong48InputZeroOutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 6;
    objectUnderTest.origin = 15;
    final int pos = 0;

    // Act
    final long actual = objectUnderTest.getUlong48(pos);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUlong48OutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getUlong48();

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUlong48OutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 13;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0,
                                (byte)0, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 51;

    // Act
    final long actual = objectUnderTest.getUlong48();

    // Assert side effects
    Assert.assertEquals(19, objectUnderTest.position);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUlong56InputNegativeOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -1_207_959_552;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getUlong56(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUlong56InputPositiveOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = 1_006_632_954;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getUlong56(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUlong56InputZeroOutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)1};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 64;
    objectUnderTest.origin = 14;
    final int pos = 0;

    // Act
    final long actual = objectUnderTest.getUlong56(pos);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUlong56OutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getUlong56();

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUlong56OutputZero() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 11;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1,
                                (byte)1, (byte)1, (byte)1, (byte)1, (byte)0, (byte)0, (byte)0,
                                (byte)0, (byte)0, (byte)0, (byte)0, (byte)1};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 18;

    // Act
    final long actual = objectUnderTest.getUlong56();

    // Assert side effects
    Assert.assertEquals(18, objectUnderTest.position);

    // Assert result
    Assert.assertEquals(0L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUlong64InputNegativeOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = -1_215_752_192;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getUlong64(pos);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void getUlong64OutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.getUlong64();

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void hasRemainingOutputFalse() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    final boolean actual = objectUnderTest.hasRemaining();

    // Assert result
    Assert.assertFalse(actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void hasRemainingOutputTrue() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    objectUnderTest.buffer = null;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 1;

    // Act
    final boolean actual = objectUnderTest.hasRemaining();

    // Assert result
    Assert.assertTrue(actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void hexdumpInputNegativeOutputNotNull() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)43, (byte)47, (byte)15, (byte)39, (byte)-81, (byte)43,
                                (byte)7,  (byte)45, (byte)45, (byte)43, (byte)15,  (byte)111,
                                (byte)46, (byte)46, (byte)14, (byte)46, (byte)47,  (byte)47,
                                (byte)47, (byte)63, (byte)43};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 2_013_265_926;
    objectUnderTest.origin = 134_217_728;
    final int pos = -134_217_711;

    // Act
    final String actual = objectUnderTest.hexdump(pos);

    // Assert result
    Assert.assertEquals("2f", actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void hexdumpInputNegativePositiveOutputNotNull() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)1,  (byte)1, (byte)16, (byte)0, (byte)0,
                                (byte)16, (byte)0, (byte)4,  (byte)1, (byte)1,
                                (byte)2,  (byte)0, (byte)64, (byte)1, (byte)-128};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 234_459_639;
    objectUnderTest.origin = 905_997_839;
    final int pos = -905_997_834;
    final int len = 2;

    // Act
    final String actual = objectUnderTest.hexdump(pos, len);

    // Assert result
    Assert.assertEquals("10_00", actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void hexdumpInputPositiveNegativeOutputNotNull() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)16, (byte)17, (byte)21, (byte)16,
                                (byte)1,  (byte)16, (byte)16, (byte)16};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 2_097_186;
    objectUnderTest.origin = 0;
    final int pos = 1;
    final int len = -1;

    // Act
    final String actual = objectUnderTest.hexdump(pos, len);

    // Assert result
    Assert.assertEquals("11", actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void hexdumpInputPositiveOutputNotNull() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = 276_617;

    // Act
    final String actual = objectUnderTest.hexdump(pos);

    // Assert result
    Assert.assertEquals("", actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void hexdumpInputPositiveZeroOutputNotNull() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int pos = 1_073_741_828;
    final int len = 0;

    // Act
    final String actual = objectUnderTest.hexdump(pos, len);

    // Assert result
    Assert.assertEquals("", actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void hexdumpInputZeroOutputNotNull() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)3,    (byte)-24, (byte)11, (byte)74,   (byte)10, (byte)-30,
                                (byte)-126, (byte)66,  (byte)10, (byte)-117, (byte)11, (byte)-21,
                                (byte)-53,  (byte)-56, (byte)98, (byte)42,   (byte)11, (byte)8,
                                (byte)-21,  (byte)-22, (byte)-6, (byte)-21,  (byte)2,  (byte)10,
                                (byte)10,   (byte)2,   (byte)-21};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 2;
    objectUnderTest.origin = 7;
    final int pos = 0;

    // Act
    final String actual = objectUnderTest.hexdump(pos);

    // Assert result
    Assert.assertEquals("42_0a", actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void limitInputNegativeOutputIllegalArgumentException() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = -2_147_483_647;
    final int newLimit = -2_147_483_648;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.limit(newLimit);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void limitInputZeroOutputIllegalArgumentException() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {(byte)0};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 1_073_741_825;
    final int newLimit = 0;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.limit(newLimit);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void limitInputZeroOutputNotNull() throws InvocationTargetException {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    objectUnderTest.position = 0;
    objectUnderTest.semival = 0;
    final byte[] myByteArray = {};
    objectUnderTest.buffer = myByteArray;
    objectUnderTest.limit = 0;
    objectUnderTest.origin = 0;
    final int newLimit = 0;

    // Act
    final LogBuffer actual = objectUnderTest.limit(newLimit);

    // Assert result
    Assert.assertNotNull(actual);
    Assert.assertEquals(0, actual.position);
    Assert.assertEquals(0, actual.semival);
    Assert.assertArrayEquals(new byte[] {}, actual.buffer);
    Assert.assertEquals(0, actual.limit);
    Assert.assertEquals(0, actual.origin);
  }

  // Test written by Diffblue Cover.
  @Test
  public void limitOutputZero() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    final int actual = objectUnderTest.limit();

    // Assert result
    Assert.assertEquals(0, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void positionInputNegativeOutputIllegalArgumentException() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int newPosition = -1_000_000_000;

    // Act
    thrown.expect(IllegalArgumentException.class);
    objectUnderTest.position(newPosition);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void positionInputZeroOutputNotNull() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();
    final int newPosition = 0;

    // Act
    final LogBuffer actual = objectUnderTest.position(newPosition);

    // Assert result
    Assert.assertNotNull(actual);
    Assert.assertEquals(0, actual.position);
    Assert.assertEquals(0, actual.semival);
    Assert.assertNull(actual.buffer);
    Assert.assertEquals(0, actual.limit);
    Assert.assertEquals(0, actual.origin);
  }

  // Test written by Diffblue Cover.
  @Test
  public void positionOutputZero() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    final int actual = objectUnderTest.position();

    // Assert result
    Assert.assertEquals(0, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void remainingOutputZero() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    final int actual = objectUnderTest.remaining();

    // Assert result
    Assert.assertEquals(0, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void rewindOutputNotNull() {

    // Arrange
    final LogBuffer objectUnderTest = new LogBuffer();

    // Act
    final LogBuffer actual = objectUnderTest.rewind();

    // Assert result
    Assert.assertNotNull(actual);
    Assert.assertEquals(0, actual.position);
    Assert.assertEquals(0, actual.semival);
    Assert.assertNull(actual.buffer);
    Assert.assertEquals(0, actual.limit);
    Assert.assertEquals(0, actual.origin);
  }
}
