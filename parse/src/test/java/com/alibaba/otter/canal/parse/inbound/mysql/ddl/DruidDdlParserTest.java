package com.alibaba.otter.canal.parse.inbound.mysql.ddl;

import static org.mockito.AdditionalMatchers.or;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.isA;
import static org.mockito.Matchers.isNull;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.otter.canal.parse.inbound.mysql.ddl.DdlResult;
import com.alibaba.otter.canal.parse.inbound.mysql.ddl.DruidDdlParser;
import com.diffblue.deeptestutils.mock.DTUMemberMatcher;
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
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

@RunWith(PowerMockRunner.class)
public class DruidDdlParserTest {

  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Rule public final Timeout globalTimeout = new Timeout(10000);

  /* testedClasses: DruidDdlParser */
  // Test written by Diffblue Cover.
  @PrepareForTest(SQLUtils.class)
  @Test
  public void parseInputNotNullNotNullOutput0() throws Exception {

    // Setup mocks
    PowerMockito.mockStatic(SQLUtils.class);

    // Arrange
    final String queryString = "a,b,c";
    final String schmeaName = "1a 2b 3c";
    final ArrayList arrayList = new ArrayList();
    final Method parseStatementsMethod = DTUMemberMatcher.method(
        SQLUtils.class, "parseStatements", String.class, DbType.class, boolean.class);
    PowerMockito.doReturn(arrayList)
        .when(SQLUtils.class, parseStatementsMethod)
        .withArguments(or(isA(String.class), isNull(String.class)),
                       or(isA(DbType.class), isNull(DbType.class)), anyBoolean());

    // Act
    final List<DdlResult> actual = DruidDdlParser.parse(queryString, schmeaName);

    // Assert result
    final ArrayList<DdlResult> arrayList1 = new ArrayList<DdlResult>();
    Assert.assertEquals(arrayList1, actual);
  }

  // Test written by Diffblue Cover.
  @PrepareForTest(SQLUtils.class)
  @Test
  public void parseInputNotNullNotNullOutput02() throws Exception {

    // Setup mocks
    PowerMockito.mockStatic(SQLUtils.class);

    // Arrange
    final String queryString = "foo";
    final String schmeaName = "BAZ";
    final ArrayList arrayList = new ArrayList();
    arrayList.add(null);
    final Method parseStatementsMethod = DTUMemberMatcher.method(
        SQLUtils.class, "parseStatements", String.class, DbType.class, boolean.class);
    PowerMockito.doReturn(arrayList)
        .when(SQLUtils.class, parseStatementsMethod)
        .withArguments(or(isA(String.class), isNull(String.class)),
                       or(isA(DbType.class), isNull(DbType.class)), anyBoolean());

    // Act
    final List<DdlResult> actual = DruidDdlParser.parse(queryString, schmeaName);

    // Assert result
    final ArrayList<DdlResult> arrayList1 = new ArrayList<DdlResult>();
    Assert.assertEquals(arrayList1, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void unescapeNameInputNotNullOutputNotNull() {

    // Arrange
    final String name = "3";

    // Act
    final String actual = DruidDdlParser.unescapeName(name);

    // Assert result
    Assert.assertEquals("3", actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void unescapeNameInputNotNullOutputNotNull2() {

    // Arrange
    final String name = "a/b/c";

    // Act
    final String actual = DruidDdlParser.unescapeName(name);

    // Assert result
    Assert.assertEquals("a/b/c", actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void unescapeNameInputNotNullOutputNotNull3() {

    // Arrange
    final String name =
        "\"\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000&";

    // Act
    final String actual = DruidDdlParser.unescapeName(name);

    // Assert result
    Assert.assertEquals(
        "\"\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000&",
        actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void unescapeNameInputNotNullOutputNotNull4() {

    // Arrange
    final String name =
        "`\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000&";

    // Act
    final String actual = DruidDdlParser.unescapeName(name);

    // Assert result
    Assert.assertEquals(
        "`\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000&",
        actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void unescapeNameInputNotNullOutputNotNull5() {

    // Arrange
    final String name = "`FFFFFFFFFFFFFF`";

    // Act
    final String actual = DruidDdlParser.unescapeName(name);

    // Assert result
    Assert.assertEquals("FFFFFFFFFFFFFF", actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void unescapeQuotaNameInputNotNullOutputNotNull() {

    // Arrange
    final String name = "3";

    // Act
    final String actual = DruidDdlParser.unescapeQuotaName(name);

    // Assert result
    Assert.assertEquals("3", actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void unescapeQuotaNameInputNotNullOutputNotNull2() {

    // Arrange
    final String name = "a,b,c";

    // Act
    final String actual = DruidDdlParser.unescapeQuotaName(name);

    // Assert result
    Assert.assertEquals("a,b,c", actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void unescapeQuotaNameInputNotNullOutputNotNull3() {

    // Arrange
    final String name = "\'\u0000\u0000\u0000\u0000/";

    // Act
    final String actual = DruidDdlParser.unescapeQuotaName(name);

    // Assert result
    Assert.assertEquals("\'\u0000\u0000\u0000\u0000/", actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void unescapeQuotaNameInputNotNullOutputNotNull4() {

    // Arrange
    final String name =
        "\'\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\'";

    // Act
    final String actual = DruidDdlParser.unescapeQuotaName(name);

    // Assert result
    Assert.assertEquals(
        "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000",
        actual);
  }
}
