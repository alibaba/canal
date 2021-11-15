package com.alibaba.otter.canal.parse.driver.pgsql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.postgresql.PGProperty;
import org.springframework.jdbc.core.RowMapper;

public class PgsqlJdbcConnector extends PgsqlConnector {

  private volatile Connection channel;

  public PgsqlJdbcConnector(String host, int port, String database, String username, String password) {
    super(host, port, database, username, password);
  }

  @Override
  public void connect() throws IOException {
    if (connected.compareAndSet(false, true)) {
      Properties props = new Properties();
      PGProperty.USER.set(props, username);
      PGProperty.PASSWORD.set(props, password);
      PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
      PGProperty.REPLICATION.set(props, "database");
      PGProperty.PREFER_QUERY_MODE.set(props, "simple");/* 1. 必须是simple格式:https://github.com/pgjdbc/pgjdbc/issues/759
                                                           2. https://github.com/pgjdbc/pgjdbc/pull/761
                                                         */
      PGProperty.TCP_KEEP_ALIVE.set(props, true);
      PGProperty.CONNECT_TIMEOUT.set(props, this.connectTimeoutInSeconds);
      PGProperty.SOCKET_TIMEOUT.set(props, this.socketTimeoutInSeconds);
      PGProperty.LOGIN_TIMEOUT.set(props, this.loginTimeoutInSeconds);
      PGProperty.SEND_BUFFER_SIZE.set(props, this.sendBufferSize);
      PGProperty.RECEIVE_BUFFER_SIZE.set(props, this.receiveBufferSize);
      String jdbcUrl = "jdbc:postgresql://" + address.getHostName() + ":" + address.getPort() + "/" + database
          + "?ApplicationName=" + System.getProperty("APPID", "")
          + "&loginTimeout=" + loginTimeoutInSeconds      // 登陆超时:How long to wait for establishment of a database.
          + "&connectTimeout=" + connectTimeoutInSeconds  // 连接超时:The timeout value used for socket connect operations.
          + "&socketTimeout=" + socketTimeoutInSeconds    // 网络读超时:The timeout value used for socket read operations.
          + "&stringtype=unspecified";                    // 不校验参数
      try {
        this.channel = DriverManager.getConnection(jdbcUrl, props);
      } catch (SQLException t) {
        throw new IOException(t);
      }
    }
  }

  @Override
  public void disconnect() {
    if (connected.compareAndSet(true, false)) {
      try {
        channel.close();
      } catch (Exception ignore) {
      }
    }
  }

  @Override
  public boolean isConnected() {
    try {
      return connected.get() && this.channel != null && !this.channel.isClosed() && this.channel.isValid(1);
    } catch (SQLException e) {
      return false;
    }
  }

  @Override
  public <T> List<T> query(String cmd, RowMapper<T> mapper, Object... args) throws IOException {
    // 这里不能使用类似:apache-commons-dbutils框架,否则有报错,因为replication connection的PreparedStatement貌似不支持setXXX
    try (PreparedStatement ps = this.channel.prepareStatement(cmd);
        ResultSet rs = ps.executeQuery();) {
      List<T> ret = new ArrayList<>();
      int i = 0;
      while (rs.next()) {
        T obj = mapper.mapRow(rs, i++);
        ret.add(obj);
      }
      return ret;
    } catch (Throwable e) {
      throw new IOException(e);
    }
  }

  @Override
  public int update(String cmd, Object... args) throws IOException {
    try (PreparedStatement ps = this.channel.prepareStatement(cmd);) {
      return ps.executeUpdate();
    } catch (Throwable e) {
      throw new IOException(e);
    }
  }


  public Connection getChannel() {
    return channel;
  }
}
