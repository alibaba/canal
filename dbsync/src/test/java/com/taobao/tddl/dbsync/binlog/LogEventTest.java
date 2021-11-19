package com.taobao.tddl.dbsync.binlog;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

public class LogEventTest {

    @Rule
    public final ExpectedException thrown        = ExpectedException.none();

    @SuppressWarnings("deprecation")
    @Rule
    public final Timeout           globalTimeout = new Timeout(10000);

    /* testedClasses: LogEvent */
    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull() {

        // Arrange
        final int type = 8;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Create_file", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull2() {

        // Arrange
        final int type = 12;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("New_load", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull3() {

        // Arrange
        final int type = 39;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Update_rows_partial", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull4() {

        // Arrange
        final int type = 36;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Unknown", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull5() {

        // Arrange
        final int type = 7;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Slave", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull6() {

        // Arrange
        final int type = 6;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Load", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull7() {

        // Arrange
        final int type = 10;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Exec_load", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull8() {

        // Arrange
        final int type = 5;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Intvar", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull9() {

        // Arrange
        final int type = 11;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Delete_file", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull10() {

        // Arrange
        final int type = 4;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Rotate", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull11() {

        // Arrange
        final int type = 13;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("RAND", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull12() {

        // Arrange
        final int type = 34;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Anonymous_Gtid", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull13() {

        // Arrange
        final int type = 15;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Format_desc", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull14() {

        // Arrange
        final int type = 32;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Delete_rows", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull15() {

        // Arrange
        final int type = 16;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Xid", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull16() {

        // Arrange
        final int type = 30;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Write_rows", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull17() {

        // Arrange
        final int type = 18;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Execute_load_query", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull18() {

        // Arrange
        final int type = 29;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Rows_query", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull19() {

        // Arrange
        final int type = 19;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Table_map", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull20() {

        // Arrange
        final int type = 28;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Ignorable", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull21() {

        // Arrange
        final int type = 33;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Gtid", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull22() {

        // Arrange
        final int type = 27;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Heartbeat", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull23() {

        // Arrange
        final int type = 26;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Incident", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull24() {

        // Arrange
        final int type = 31;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Update_rows", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull25() {

        // Arrange
        final int type = 25;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Delete_rows_v1", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull26() {

        // Arrange
        final int type = 24;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Update_rows_v1", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull27() {

        // Arrange
        final int type = 23;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Write_rows_v1", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull28() {

        // Arrange
        final int type = 35;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Previous_gtids", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull29() {

        // Arrange
        final int type = 22;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Delete_rows_event_old", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull30() {

        // Arrange
        final int type = 21;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Update_rows_event_old", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull31() {

        // Arrange
        final int type = 20;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Write_rows_event_old", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull32() {

        // Arrange
        final int type = 17;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Begin_load_query", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull33() {

        // Arrange
        final int type = 14;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("User var", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull34() {

        // Arrange
        final int type = 9;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Append_block", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull35() {

        // Arrange
        final int type = 2;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Query", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull36() {

        // Arrange
        final int type = 3;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Stop", actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getTypeNameInputPositiveOutputNotNull37() {

        // Arrange
        final int type = 1;

        // Act
        final String actual = LogEvent.getTypeName(type);

        // Assert result
        Assert.assertEquals("Start_v3", actual);
    }
}
