package com.alibaba.otter.canal.client.adapter.support;

import static org.mockito.AdditionalMatchers.or;
import static org.mockito.Matchers.isA;
import static org.mockito.Matchers.isNull;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import com.alibaba.otter.canal.client.adapter.support.JdbcTypeUtil;
import com.alibaba.otter.canal.client.adapter.support.Util;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Array;

@RunWith(PowerMockRunner.class)
public class JdbcTypeUtilTest {

  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Rule public final Timeout globalTimeout = new Timeout(10000);

  /* testedClasses: JdbcTypeUtil */
  // Test written by Diffblue Cover.
  @Test
  public void typeConvertInputNotNullNotNullNotNullNegativeNotNullOutput3() {

    // Arrange
    final String tableName = "foo";
    final String columnName = "foo";
    final String value = "foo";
    final int sqlType = -4;
    final String mysqlType = "foo";

    // Act
    final Object actual =
        JdbcTypeUtil.typeConvert(tableName, columnName, value, sqlType, mysqlType);

    // Assert result
    Assert.assertArrayEquals(new byte[] {(byte)102, (byte)111, (byte)111}, ((byte[])actual));
  }

  // Test written by Diffblue Cover.
  @Test
  public void typeConvertInputNotNullNotNullNotNullNegativeNotNullOutputPositive() {

    // Arrange
    final String tableName = "foo";
    final String columnName = "foo";
    final String value = "1234";
    final int sqlType = -5;
    final String mysqlType = "foo";

    // Act
    final Object actual =
        JdbcTypeUtil.typeConvert(tableName, columnName, value, sqlType, mysqlType);

    // Assert result
    Assert.assertEquals(1234L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void typeConvertInputNotNullNotNullNotNullNegativeNotNullOutputPositive2() {

    // Arrange
    final String tableName = "?????????";
    final String columnName = "?";
    final String value = "2";
    final int sqlType = -5;
    final String mysqlType = "bigint";

    // Act
    final Object actual =
        JdbcTypeUtil.typeConvert(tableName, columnName, value, sqlType, mysqlType);

    // Assert result
    Assert.assertEquals(2L, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void typeConvertInputNotNullNotNullNotNullPositiveNotNullOutputFalse() {

    // Arrange
    final String tableName = "?????????";
    final String columnName = "?";
    final String value = "0";
    final int sqlType = 16;
    final String mysqlType = "bigint\u046bunsigned";

    // Act
    final Object actual =
        JdbcTypeUtil.typeConvert(tableName, columnName, value, sqlType, mysqlType);

    // Assert result
    Assert.assertFalse((boolean)actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void typeConvertInputNotNullNotNullNotNullPositiveNotNullOutputNotNull() {

    // Arrange
    final String tableName = "1";
    final String columnName = "Bar";
    final String value = "1";
    final int sqlType = 12;
    final String mysqlType = "2";

    // Act
    final Object actual =
        JdbcTypeUtil.typeConvert(tableName, columnName, value, sqlType, mysqlType);

    // Assert result
    Assert.assertEquals("1", actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void typeConvertInputNotNullNotNullNotNullPositiveNotNullOutputPositive() {

    // Arrange
    final String tableName = "foo";
    final String columnName = "foo";
    final String value = "3";
    final int sqlType = 4;
    final String mysqlType = "foo";

    // Act
    final Object actual =
        JdbcTypeUtil.typeConvert(tableName, columnName, value, sqlType, mysqlType);

    // Assert result
    Assert.assertEquals(3, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void typeConvertInputNotNullNotNullNotNullPositiveNotNullOutputTrue() {

    // Arrange
    final String tableName = "?????????";
    final String columnName = "?";
    final String value = "5";
    final int sqlType = 16;
    final String mysqlType = "bigintsunsigned";

    // Act
    final Object actual =
        JdbcTypeUtil.typeConvert(tableName, columnName, value, sqlType, mysqlType);

    // Assert result
    Assert.assertTrue((boolean)actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void typeConvertInputNotNullNotNullNotNullPositiveNotNullOutputZero2() {

    // Arrange
    final String tableName = "?????????";
    final String columnName = "?";
    final String value = "0";
    final int sqlType = 7;
    final String mysqlType = "bigint\u046bunsigned";

    // Act
    final Object actual =
        JdbcTypeUtil.typeConvert(tableName, columnName, value, sqlType, mysqlType);

    // Assert result
    Assert.assertEquals(0.0f, (float)actual, 0.0f);
  }

  // Test written by Diffblue Cover.
  @Test
  public void typeConvertInputNotNullNotNullNotNullPositiveNotNullOutputZero3() {

    // Arrange
    final String tableName = "?????????";
    final String columnName = "?";
    final String value = "0";
    final int sqlType = 5;
    final String mysqlType = "bigintiunsigned";

    // Act
    final Object actual =
        JdbcTypeUtil.typeConvert(tableName, columnName, value, sqlType, mysqlType);

    // Assert result
    Assert.assertEquals((short)0, actual);
  }
}
