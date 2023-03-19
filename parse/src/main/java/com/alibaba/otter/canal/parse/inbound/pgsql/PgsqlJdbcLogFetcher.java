package com.alibaba.otter.canal.parse.inbound.pgsql;

import com.alibaba.otter.canal.parse.driver.pgsql.PgsqlJdbcConnector;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class PgsqlJdbcLogFetcher extends PgsqlLogFetcher {

  protected Connection jdbcConnection;

  @Override
  protected final void doStart(Long pos) throws SQLException {
    this.jdbcConnection = ((PgsqlJdbcConnector) pgsqlConnection.connector).getChannel();
    checkDestination();
    start0(pos);
  }

  protected abstract void start0(Long pos) throws SQLException;

  private void checkDestination() throws SQLException {
    String countSql = "select count(*) as t from pg_replication_slots where slot_name='" + this.destination + "'";
    String createSql = "select * from pg_create_logical_replication_slot('" + this.destination + "', 'test_decoding')";
    boolean exists = false;
    try (PreparedStatement ps = jdbcConnection.prepareStatement(countSql);
        ResultSet rs = ps.executeQuery();) {
      while (rs.next()) {
        int total = rs.getInt("t");
        exists = total == 1;
      }
    }
    if (exists) {
      return;
    }
    try (PreparedStatement ps = jdbcConnection.prepareStatement(createSql);
        ResultSet rs = ps.executeQuery();) {
      while (rs.next()) {
        String name = rs.getString("slot_name");
        String lsn = rs.getString("lsn");
      }
    }
  }

}
