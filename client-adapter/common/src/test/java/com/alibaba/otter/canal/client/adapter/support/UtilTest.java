package com.alibaba.otter.canal.client.adapter.support;

import static org.mockito.AdditionalMatchers.or;
import static org.mockito.Matchers.isA;
import static org.mockito.Matchers.isNull;

import com.diffblue.deeptestutils.mock.DTUMemberMatcher;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Method;
import java.util.Date;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*"})
public class UtilTest {

  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Rule public final Timeout globalTimeout = new Timeout(10000);

  /* testedClasses: Util */
  // Test written by Diffblue Cover.
  @Test
  public void cleanColumnInputNotNullOutputNotNull() {

    // Arrange
    final String column = "1234";

    // Act
    final String actual = Util.cleanColumn(column);

    // Assert result
    Assert.assertEquals("1234", actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void cleanColumnInputNullOutputNull() {

    // Arrange
    final String column = null;

    // Act
    final String actual = Util.cleanColumn(column);

    // Assert result
    Assert.assertNull(actual);
  }

  // Test written by Diffblue Cover.
  @PrepareForTest(StringUtils.class)
  @Test
  public void parseDate2InputNotNullOutputNull() throws Exception {

    // Setup mocks
    PowerMockito.mockStatic(StringUtils.class);

    // Arrange
    final String datetimeStr = "1a 2b 3c";
    final Method isEmptyMethod =
        DTUMemberMatcher.method(StringUtils.class, "isEmpty", String.class);
    PowerMockito.doReturn(true)
        .when(StringUtils.class, isEmptyMethod)
        .withArguments(or(isA(String.class), isNull(String.class)));

    // Act
    final Date actual = Util.parseDate2(datetimeStr);

    // Assert result
    Assert.assertNull(actual);
  }

  // Test written by Diffblue Cover.
  @PrepareForTest(StringUtils.class)
  @Test
  public void parseDate2InputNotNullOutputNull2() throws Exception {

    // Setup mocks
    PowerMockito.mockStatic(StringUtils.class);

    // Arrange
    final String datetimeStr = "1a 2b 3c";
    final Method isEmptyMethod =
        DTUMemberMatcher.method(StringUtils.class, "isEmpty", String.class);
    PowerMockito.doReturn(false)
        .when(StringUtils.class, isEmptyMethod)
        .withArguments(or(isA(String.class), isNull(String.class)));

    // Act
    final Date actual = Util.parseDate2(datetimeStr);

    // Assert result
    Assert.assertNull(actual);
  }

  // Test written by Diffblue Cover.
  @PrepareForTest(StringUtils.class)
  @Test
  public void parseDateInputNotNullOutputNull() throws Exception {

    // Setup mocks
    PowerMockito.mockStatic(StringUtils.class);

    // Arrange
    final String datetimeStr = "a/b/c";
    final Method isEmptyMethod =
        DTUMemberMatcher.method(StringUtils.class, "isEmpty", String.class);
    PowerMockito.doReturn(true)
        .when(StringUtils.class, isEmptyMethod)
        .withArguments(or(isA(String.class), isNull(String.class)));

    // Act
    final Date actual = Util.parseDate(datetimeStr);

    // Assert result
    Assert.assertNull(actual);
  }
}
