package com.alibaba.otter.canal.parse.inbound.mysql.ddl;

import com.alibaba.otter.canal.parse.inbound.mysql.ddl.DdlResult;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

import java.lang.reflect.InvocationTargetException;

public class DdlResultTest {

  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Rule public final Timeout globalTimeout = new Timeout(10000);

  /* testedClasses: DdlResult */
  // Test written by Diffblue Cover.
  @Test
  public void cloneOutputNotNull() {

    // Arrange
    final DdlResult objectUnderTest = new DdlResult();

    // Act
    final DdlResult actual = objectUnderTest.clone();

    // Assert result
    Assert.assertNotNull(actual);
    Assert.assertNull(actual.getSchemaName());
    Assert.assertNull(actual.getType());
    Assert.assertNull(actual.getOriSchemaName());
    Assert.assertNull(actual.getTableName());
    Assert.assertNull(actual.getRenameTableResult());
    Assert.assertNull(actual.getOriTableName());
  }

  // Test written by Diffblue Cover.

  @Test
  public void constructorInputNotNullNotNullNotNullNotNullOutputVoid() {

    // Arrange
    final String schemaName = "foo";
    final String tableName = "\'";
    final String oriSchemaName = "1a 2b 3c";
    final String oriTableName = "BAZ";

    // Act, creating object to test constructor
    final DdlResult objectUnderTest =
        new DdlResult(schemaName, tableName, oriSchemaName, oriTableName);

    // Assert side effects
    Assert.assertEquals("foo", objectUnderTest.getSchemaName());
    Assert.assertEquals("1a 2b 3c", objectUnderTest.getOriSchemaName());
    Assert.assertEquals("\'", objectUnderTest.getTableName());
    Assert.assertEquals("BAZ", objectUnderTest.getOriTableName());
  }

  // Test written by Diffblue Cover.

  @Test
  public void constructorInputNotNullNotNullOutputVoid() {

    // Arrange
    final String schemaName = ",";
    final String tableName = "BAZ";

    // Act, creating object to test constructor
    final DdlResult objectUnderTest = new DdlResult(schemaName, tableName);

    // Assert side effects
    Assert.assertEquals(",", objectUnderTest.getSchemaName());
    Assert.assertEquals("BAZ", objectUnderTest.getTableName());
  }

  // Test written by Diffblue Cover.

  @Test
  public void constructorInputNotNullOutputVoid() {

    // Arrange
    final String schemaName = "3";

    // Act, creating object to test constructor
    final DdlResult objectUnderTest = new DdlResult(schemaName);

    // Assert side effects
    Assert.assertEquals("3", objectUnderTest.getSchemaName());
  }

  // Test written by Diffblue Cover.
  @Test
  public void getOriSchemaNameOutputNull() {

    // Arrange
    final DdlResult objectUnderTest = new DdlResult();

    // Act
    final String actual = objectUnderTest.getOriSchemaName();

    // Assert result
    Assert.assertNull(actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getOriTableNameOutputNull() {

    // Arrange
    final DdlResult objectUnderTest = new DdlResult();

    // Act
    final String actual = objectUnderTest.getOriTableName();

    // Assert result
    Assert.assertNull(actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getRenameTableResultOutputNull() {

    // Arrange
    final DdlResult objectUnderTest = new DdlResult();

    // Act
    final DdlResult actual = objectUnderTest.getRenameTableResult();

    // Assert result
    Assert.assertNull(actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getSchemaNameOutputNull() {

    // Arrange
    final DdlResult objectUnderTest = new DdlResult();

    // Act
    final String actual = objectUnderTest.getSchemaName();

    // Assert result
    Assert.assertNull(actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getTableNameOutputNull() {

    // Arrange
    final DdlResult objectUnderTest = new DdlResult();

    // Act
    final String actual = objectUnderTest.getTableName();

    // Assert result
    Assert.assertNull(actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getTypeOutputNull() {

    // Arrange
    final DdlResult objectUnderTest = new DdlResult();

    // Act
    final EventType actual = objectUnderTest.getType();

    // Assert result
    Assert.assertNull(actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void setOriSchemaNameInputNotNullOutputVoid() {

    // Arrange
    final DdlResult objectUnderTest = new DdlResult();
    final String oriSchemaName = "3";

    // Act
    objectUnderTest.setOriSchemaName(oriSchemaName);

    // Assert side effects
    Assert.assertEquals("3", objectUnderTest.getOriSchemaName());
  }

  // Test written by Diffblue Cover.
  @Test
  public void setOriTableNameInputNotNullOutputVoid() {

    // Arrange
    final DdlResult objectUnderTest = new DdlResult();
    final String oriTableName = "3";

    // Act
    objectUnderTest.setOriTableName(oriTableName);

    // Assert side effects
    Assert.assertEquals("3", objectUnderTest.getOriTableName());
  }

  // Test written by Diffblue Cover.
  @Test
  public void setSchemaNameInputNotNullOutputVoid() {

    // Arrange
    final DdlResult objectUnderTest = new DdlResult();
    final String schemaName = "3";

    // Act
    objectUnderTest.setSchemaName(schemaName);

    // Assert side effects
    Assert.assertEquals("3", objectUnderTest.getSchemaName());
  }

  // Test written by Diffblue Cover.
  @Test
  public void setTableNameInputNotNullOutputVoid() {

    // Arrange
    final DdlResult objectUnderTest = new DdlResult();
    final String tableName = "3";

    // Act
    objectUnderTest.setTableName(tableName);

    // Assert side effects
    Assert.assertEquals("3", objectUnderTest.getTableName());
  }

  // Test written by Diffblue Cover.
  @Test
  public void toStringOutputNotNull() {

    // Arrange
    final DdlResult objectUnderTest = new DdlResult();

    // Act
    final String actual = objectUnderTest.toString();

    // Assert result
    Assert.assertEquals(
        "DdlResult [schemaName=null , tableName=null , oriSchemaName=null , oriTableName=null , type=null ];",
        actual);
  }

  // Test written by Diffblue Cover.

  @Test
  public void toStringOutputNotNull2() throws InvocationTargetException {

    // Arrange
    final DdlResult objectUnderTest = new DdlResult();
    objectUnderTest.setSchemaName(null);
    objectUnderTest.setType(null);
    objectUnderTest.setOriSchemaName(null);
    objectUnderTest.setTableName(",");
    final DdlResult ddlResult = new DdlResult();
    objectUnderTest.setRenameTableResult(ddlResult);
    objectUnderTest.setOriTableName("2");

    // Act
    final String actual = objectUnderTest.toString();

    // Assert result
    Assert.assertEquals(
        "DdlResult [schemaName=null , tableName=, , oriSchemaName=null , oriTableName=2 , type=null ];DdlResult [schemaName=null , tableName=null , oriSchemaName=null , oriTableName=null , type=null ];",
        actual);
  }
}
