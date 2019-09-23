package com.taobao.tddl.dbsync.binlog.event;

import static org.mockito.AdditionalMatchers.or;
import static org.mockito.Matchers.isA;
import static org.mockito.Matchers.isNull;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import com.diffblue.deeptestutils.mock.DTUMemberMatcher;
import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.event.RowsLogBuffer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.BitSet;

@RunWith(PowerMockRunner.class)
public class RowsLogBufferTest {

  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Rule public final Timeout globalTimeout = new Timeout(10000);

  /* testedClasses: RowsLogBuffer */
  // Test written by Diffblue Cover.
  @PrepareForTest(LogFactory.class)
  @Test
  public void appendNumber3InputNotNullPositiveOutputArrayIndexOutOfBoundsException()
      throws Exception {

    // Setup mocks
    PowerMockito.mockStatic(LogFactory.class);

    // Arrange
    final StringBuilder builder = new StringBuilder("????");
    final int d = 1_006_586_465;
    final Method getLogMethod = DTUMemberMatcher.method(LogFactory.class, "getLog", Class.class);
    PowerMockito.doReturn(null)
        .when(LogFactory.class, getLogMethod)
        .withArguments(or(isA(Class.class), isNull(Class.class)));

    // Act
    thrown.expect(ArrayIndexOutOfBoundsException.class);
    RowsLogBuffer.appendNumber3(builder, d);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @PrepareForTest(LogFactory.class)
  @Test
  public void appendNumber3InputNotNullPositiveOutputVoid() throws Exception {

    // Setup mocks
    PowerMockito.mockStatic(LogFactory.class);

    // Arrange
    final StringBuilder builder = new StringBuilder("????");
    final int d = 865;
    final Method getLogMethod = DTUMemberMatcher.method(LogFactory.class, "getLog", Class.class);
    PowerMockito.doReturn(null)
        .when(LogFactory.class, getLogMethod)
        .withArguments(or(isA(Class.class), isNull(Class.class)));

    // Act
    RowsLogBuffer.appendNumber3(builder, d);

    // Assert side effects
    Assert.assertNotNull(builder);
    Assert.assertEquals("????865", builder.toString());
  }

  // Test written by Diffblue Cover.
  @PrepareForTest(LogFactory.class)
  @Test
  public void appendNumber3InputNotNullPositiveOutputVoid2() throws Exception {

    // Setup mocks
    PowerMockito.mockStatic(LogFactory.class);

    // Arrange
    final StringBuilder builder = new StringBuilder("????");
    final int d = 1;
    final Method getLogMethod = DTUMemberMatcher.method(LogFactory.class, "getLog", Class.class);
    PowerMockito.doReturn(null)
        .when(LogFactory.class, getLogMethod)
        .withArguments(or(isA(Class.class), isNull(Class.class)));

    // Act
    RowsLogBuffer.appendNumber3(builder, d);

    // Assert side effects
    Assert.assertNotNull(builder);
    Assert.assertEquals("????001", builder.toString());
  }

  // Test written by Diffblue Cover.
  @PrepareForTest(LogFactory.class)
  @Test
  public void appendNumber4InputNotNullNegativeOutputArrayIndexOutOfBoundsException()
      throws Throwable {

    // Setup mocks
    PowerMockito.mockStatic(LogFactory.class);

    // Arrange
    final StringBuilder builder = new StringBuilder("??");
    final int d = -788_530_017;
    final Method getLogMethod = DTUMemberMatcher.method(LogFactory.class, "getLog", Class.class);
    PowerMockito.doReturn(null)
        .when(LogFactory.class, getLogMethod)
        .withArguments(or(isA(Class.class), isNull(Class.class)));

    // Act
    thrown.expect(ArrayIndexOutOfBoundsException.class);
    try {
      RowsLogBuffer.appendNumber4(builder, d);
    } catch (java.lang.ArrayIndexOutOfBoundsException ex) {

      // Assert side effects
      Assert.assertNotNull(builder);
      Assert.assertEquals("??000", builder.toString());
      throw ex;
    }
  }

  // Test written by Diffblue Cover.
  @PrepareForTest(LogFactory.class)
  @Test
  public void appendNumber4InputNotNullPositiveOutputArrayIndexOutOfBoundsException()
      throws Exception {

    // Setup mocks
    PowerMockito.mockStatic(LogFactory.class);

    // Arrange
    final StringBuilder builder = new StringBuilder("??");
    final int d = 788_529_997;
    final Method getLogMethod = DTUMemberMatcher.method(LogFactory.class, "getLog", Class.class);
    PowerMockito.doReturn(null)
        .when(LogFactory.class, getLogMethod)
        .withArguments(or(isA(Class.class), isNull(Class.class)));

    // Act
    thrown.expect(ArrayIndexOutOfBoundsException.class);
    RowsLogBuffer.appendNumber4(builder, d);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @PrepareForTest(LogFactory.class)
  @Test
  public void appendNumber4InputNotNullPositiveOutputVoid() throws Exception {

    // Setup mocks
    PowerMockito.mockStatic(LogFactory.class);

    // Arrange
    final StringBuilder builder = new StringBuilder("??");
    final int d = 14;
    final Method getLogMethod = DTUMemberMatcher.method(LogFactory.class, "getLog", Class.class);
    PowerMockito.doReturn(null)
        .when(LogFactory.class, getLogMethod)
        .withArguments(or(isA(Class.class), isNull(Class.class)));

    // Act
    RowsLogBuffer.appendNumber4(builder, d);

    // Assert side effects
    Assert.assertNotNull(builder);
    Assert.assertEquals("??0014", builder.toString());
  }

  // Test written by Diffblue Cover.
  @PrepareForTest(LogFactory.class)
  @Test
  public void appendNumber4InputNotNullPositiveOutputVoid2() throws Exception {

    // Setup mocks
    PowerMockito.mockStatic(LogFactory.class);

    // Arrange
    final StringBuilder builder = new StringBuilder("??");
    final int d = 1000;
    final Method getLogMethod = DTUMemberMatcher.method(LogFactory.class, "getLog", Class.class);
    PowerMockito.doReturn(null)
        .when(LogFactory.class, getLogMethod)
        .withArguments(or(isA(Class.class), isNull(Class.class)));

    // Act
    RowsLogBuffer.appendNumber4(builder, d);

    // Assert side effects
    Assert.assertNotNull(builder);
    Assert.assertEquals("??1000", builder.toString());
  }

  // Test written by Diffblue Cover.
  @Test
  public void mysqlToJavaTypeInputPositivePositiveFalseOutputNegative() {

    // Arrange
    final int type = 1;
    final int meta = 60_928;
    final boolean isBinary = false;

    // Act
    final int actual = RowsLogBuffer.mysqlToJavaType(type, meta, isBinary);

    // Assert result
    Assert.assertEquals(-6, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void mysqlToJavaTypeInputPositivePositiveFalseOutputNegative2() {

    // Arrange
    final int type = 248;
    final int meta = 60_928;
    final boolean isBinary = false;

    // Act
    final int actual = RowsLogBuffer.mysqlToJavaType(type, meta, isBinary);

    // Assert result
    Assert.assertEquals(-2, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void mysqlToJavaTypeInputPositivePositiveFalseOutputNegative3() {

    // Arrange
    final int type = 255;
    final int meta = 60_928;
    final boolean isBinary = false;

    // Act
    final int actual = RowsLogBuffer.mysqlToJavaType(type, meta, isBinary);

    // Assert result
    Assert.assertEquals(-2, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void mysqlToJavaTypeInputPositivePositiveFalseOutputNegative4() {

    // Arrange
    final int type = 8;
    final int meta = 60_928;
    final boolean isBinary = false;

    // Act
    final int actual = RowsLogBuffer.mysqlToJavaType(type, meta, isBinary);

    // Assert result
    Assert.assertEquals(-5, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void mysqlToJavaTypeInputPositivePositiveFalseOutputNegative5() {

    // Arrange
    final int type = 16;
    final int meta = 60_928;
    final boolean isBinary = false;

    // Act
    final int actual = RowsLogBuffer.mysqlToJavaType(type, meta, isBinary);

    // Assert result
    Assert.assertEquals(-7, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void mysqlToJavaTypeInputPositivePositiveFalseOutputNegative6() {

    // Arrange
    final int type = 251;
    final int meta = 1;
    final boolean isBinary = false;

    // Act
    final int actual = RowsLogBuffer.mysqlToJavaType(type, meta, isBinary);

    // Assert result
    Assert.assertEquals(-3, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void mysqlToJavaTypeInputPositivePositiveFalseOutputNegative7() {

    // Arrange
    final int type = 251;
    final int meta = 129;
    final boolean isBinary = false;

    // Act
    final int actual = RowsLogBuffer.mysqlToJavaType(type, meta, isBinary);

    // Assert result
    Assert.assertEquals(-4, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void mysqlToJavaTypeInputPositivePositiveFalseOutputPositive() {

    // Arrange
    final int type = 254;
    final int meta = 12_288;
    final boolean isBinary = false;

    // Act
    final int actual = RowsLogBuffer.mysqlToJavaType(type, meta, isBinary);

    // Assert result
    Assert.assertEquals(1, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void mysqlToJavaTypeInputPositivePositiveFalseOutputPositive2() {

    // Arrange
    final int type = 254;
    final int meta = 60_928;
    final boolean isBinary = false;

    // Act
    final int actual = RowsLogBuffer.mysqlToJavaType(type, meta, isBinary);

    // Assert result
    Assert.assertEquals(1, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void mysqlToJavaTypeInputPositivePositiveFalseOutputPositive3() {

    // Arrange
    final int type = 19;
    final int meta = 60_928;
    final boolean isBinary = false;

    // Act
    final int actual = RowsLogBuffer.mysqlToJavaType(type, meta, isBinary);

    // Assert result
    Assert.assertEquals(92, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void mysqlToJavaTypeInputPositivePositiveFalseOutputPositive4() {

    // Arrange
    final int type = 14;
    final int meta = 60_928;
    final boolean isBinary = false;

    // Act
    final int actual = RowsLogBuffer.mysqlToJavaType(type, meta, isBinary);

    // Assert result
    Assert.assertEquals(91, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void mysqlToJavaTypeInputPositivePositiveFalseOutputPositive5() {

    // Arrange
    final int type = 18;
    final int meta = 60_928;
    final boolean isBinary = false;

    // Act
    final int actual = RowsLogBuffer.mysqlToJavaType(type, meta, isBinary);

    // Assert result
    Assert.assertEquals(93, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void mysqlToJavaTypeInputPositivePositiveFalseOutputPositive6() {

    // Arrange
    final int type = 5;
    final int meta = 60_928;
    final boolean isBinary = false;

    // Act
    final int actual = RowsLogBuffer.mysqlToJavaType(type, meta, isBinary);

    // Assert result
    Assert.assertEquals(8, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void mysqlToJavaTypeInputPositivePositiveFalseOutputPositive7() {

    // Arrange
    final int type = 247;
    final int meta = 60_928;
    final boolean isBinary = false;

    // Act
    final int actual = RowsLogBuffer.mysqlToJavaType(type, meta, isBinary);

    // Assert result
    Assert.assertEquals(4, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void mysqlToJavaTypeInputPositivePositiveFalseOutputPositive8() {

    // Arrange
    final int type = 13;
    final int meta = 60_928;
    final boolean isBinary = false;

    // Act
    final int actual = RowsLogBuffer.mysqlToJavaType(type, meta, isBinary);

    // Assert result
    Assert.assertEquals(12, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void mysqlToJavaTypeInputPositivePositiveFalseOutputPositive9() {

    // Arrange
    final int type = 246;
    final int meta = 60_928;
    final boolean isBinary = false;

    // Act
    final int actual = RowsLogBuffer.mysqlToJavaType(type, meta, isBinary);

    // Assert result
    Assert.assertEquals(3, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void mysqlToJavaTypeInputPositivePositiveFalseOutputPositive10() {

    // Arrange
    final int type = 9;
    final int meta = 60_928;
    final boolean isBinary = false;

    // Act
    final int actual = RowsLogBuffer.mysqlToJavaType(type, meta, isBinary);

    // Assert result
    Assert.assertEquals(4, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void mysqlToJavaTypeInputPositivePositiveFalseOutputPositive11() {

    // Arrange
    final int type = 2;
    final int meta = 60_928;
    final boolean isBinary = false;

    // Act
    final int actual = RowsLogBuffer.mysqlToJavaType(type, meta, isBinary);

    // Assert result
    Assert.assertEquals(5, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void mysqlToJavaTypeInputPositivePositiveFalseOutputPositive12() {

    // Arrange
    final int type = 6;
    final int meta = 60_928;
    final boolean isBinary = false;

    // Act
    final int actual = RowsLogBuffer.mysqlToJavaType(type, meta, isBinary);

    // Assert result
    Assert.assertEquals(1111, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void mysqlToJavaTypeInputPositivePositiveFalseOutputPositive13() {

    // Arrange
    final int type = 4;
    final int meta = 60_928;
    final boolean isBinary = false;

    // Act
    final int actual = RowsLogBuffer.mysqlToJavaType(type, meta, isBinary);

    // Assert result
    Assert.assertEquals(7, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void mysqlToJavaTypeInputPositivePositiveFalseOutputPositive14() {

    // Arrange
    final int type = 3;
    final int meta = 60_928;
    final boolean isBinary = false;

    // Act
    final int actual = RowsLogBuffer.mysqlToJavaType(type, meta, isBinary);

    // Assert result
    Assert.assertEquals(4, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void mysqlToJavaTypeInputPositivePositiveFalseOutputPositive15() {

    // Arrange
    final int type = 253;
    final int meta = 129;
    final boolean isBinary = false;

    // Act
    final int actual = RowsLogBuffer.mysqlToJavaType(type, meta, isBinary);

    // Assert result
    Assert.assertEquals(12, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void mysqlToJavaTypeInputPositivePositiveTrueOutputNegative() {

    // Arrange
    final int type = 254;
    final int meta = 12_288;
    final boolean isBinary = true;

    // Act
    final int actual = RowsLogBuffer.mysqlToJavaType(type, meta, isBinary);

    // Assert result
    Assert.assertEquals(-2, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void mysqlToJavaTypeInputPositivePositiveTrueOutputNegative2() {

    // Arrange
    final int type = 254;
    final int meta = 65_024;
    final boolean isBinary = true;

    // Act
    final int actual = RowsLogBuffer.mysqlToJavaType(type, meta, isBinary);

    // Assert result
    Assert.assertEquals(-2, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void mysqlToJavaTypeInputPositivePositiveTrueOutputNegative3() {

    // Arrange
    final int type = 253;
    final int meta = 129;
    final boolean isBinary = true;

    // Act
    final int actual = RowsLogBuffer.mysqlToJavaType(type, meta, isBinary);

    // Assert result
    Assert.assertEquals(-3, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void mysqlToJavaTypeInputPositiveZeroFalseOutputPositive() {

    // Arrange
    final int type = 254;
    final int meta = 0;
    final boolean isBinary = false;

    // Act
    final int actual = RowsLogBuffer.mysqlToJavaType(type, meta, isBinary);

    // Assert result
    Assert.assertEquals(1, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void mysqlToJavaTypeInputZeroPositiveTrueOutputPositive() {

    // Arrange
    final int type = 0;
    final int meta = 65_024;
    final boolean isBinary = true;

    // Act
    final int actual = RowsLogBuffer.mysqlToJavaType(type, meta, isBinary);

    // Assert result
    Assert.assertEquals(3, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void usecondsToStrInputNegativeZeroOutputNotNull() {

    // Arrange
    final int frac = -80_098;
    final int meta = 0;

    // Act
    final String actual = RowsLogBuffer.usecondsToStr(frac, meta);

    // Assert result
    Assert.assertEquals("", actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void usecondsToStrInputPositiveNegativeOutputStringIndexOutOfBoundsException() {

    // Arrange
    final int frac = 8_525_675;
    final int meta = -2_147_483_647;

    // Act
    thrown.expect(StringIndexOutOfBoundsException.class);
    RowsLogBuffer.usecondsToStr(frac, meta);

    // Method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.
  @Test
  public void usecondsToStrInputPositivePositiveOutputIllegalArgumentException() {

    // Arrange
    final int frac = 9;
    final int meta = 104;

    // Act
    thrown.expect(IllegalArgumentException.class);
    RowsLogBuffer.usecondsToStr(frac, meta);

    // Method is not expected to return due to exception thrown
  }
}
