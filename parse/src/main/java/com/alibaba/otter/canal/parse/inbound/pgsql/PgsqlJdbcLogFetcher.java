package com.alibaba.otter.canal.parse.inbound.pgsql;

import com.alibaba.otter.canal.parse.driver.pgsql.PgsqlJdbcConnector;
import java.sql.Connection;
import java.sql.SQLException;

public abstract class PgsqlJdbcLogFetcher extends PgsqlLogFetcher {

  protected Connection jdbcConnection;

  @Override
  protected final void doStart(Long pos) throws SQLException {
    this.jdbcConnection = ((PgsqlJdbcConnector) pgsqlConnection.connector).getChannel();
    start0(pos);
  }

  protected abstract void start0(Long pos) throws SQLException;
}
