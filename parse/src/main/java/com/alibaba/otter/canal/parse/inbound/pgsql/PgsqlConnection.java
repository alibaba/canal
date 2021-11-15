package com.alibaba.otter.canal.parse.inbound.pgsql;

import com.alibaba.otter.canal.parse.driver.mysql.packets.GTIDSet;
import com.alibaba.otter.canal.parse.driver.pgsql.PgsqlConnector;
import com.alibaba.otter.canal.parse.inbound.ErosaConnection;
import com.alibaba.otter.canal.parse.inbound.MultiStageCoprocessor;
import com.alibaba.otter.canal.parse.inbound.SinkFunction;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;

public abstract class PgsqlConnection implements ErosaConnection, AutoCloseable {

  protected final Logger logger = LoggerFactory.getLogger(getClass());
  protected final AuthenticationInfo authInfo;
  private final AtomicBoolean connected = new AtomicBoolean(false);
  protected int connectTimeoutInSeconds = 60;         // connectTimeout. The timeout value used for socket connect operations.
  protected int loginTimeoutInSeconds = 60;           // loginTimeout. How long to wait for establishment of a database.
  protected int socketTimeoutInSeconds = 60;          // socketTimeout. The timeout value used for socket read operations.
  protected int receiveBufferSize = 64 * 1024 * 1024; // 64MB
  protected int sendBufferSize = 64 * 1024 * 1024;    // 64MB
  protected PgsqlConnector connector;
  protected PgsqlConnectionStat stat;

  public PgsqlConnection(String host, int port, String database, String username, String password) {
    this(new InetSocketAddress(host, port), database, username, password);
  }

  public PgsqlConnection(InetSocketAddress address, String database, String username, String password) {
    this.authInfo = new AuthenticationInfo(address, username, password, database);
  }

  @Override
  public final void dump(GTIDSet gtidSet, SinkFunction func) throws IOException {
    throw new UnsupportedOperationException("## pgsql unsupported this function now");
  }

  @Override
  public final void dump(GTIDSet gtidSet, MultiStageCoprocessor coprocessor) throws IOException {
    throw new UnsupportedOperationException("## pgsql unsupported this function now");
  }

  @Override
  public final PgsqlConnection fork() {
    PgsqlConnection connection = doFork();
    connection.loginTimeoutInSeconds = (loginTimeoutInSeconds);
    connection.socketTimeoutInSeconds = (socketTimeoutInSeconds);
    connection.connectTimeoutInSeconds = (connectTimeoutInSeconds);
    connection.receiveBufferSize = (receiveBufferSize);
    connection.sendBufferSize = (sendBufferSize);
    return connection;
  }

  @Override
  public final void reconnect() throws IOException {
    disconnect();
    connect();
  }

  @Override
  public final void connect() throws IOException {
    if (connected.compareAndSet(false, true)) {
      doConnect();
      this.stat = getStat();
    }
  }

  @Override
  public final void disconnect() {
    if (connected.compareAndSet(true, false)) {
      doDisconnect();
      this.stat = null;
    }
  }

  @Override
  public abstract String toString();

  @Override
  public long queryServerId() {
    return this.stat == null ? -1 : this.stat.getServerId();
  }

  @Override
  public final void close() {
    disconnect();
  }

  public abstract boolean isConnected();

  public abstract List<Map<String, Object>> query(String cmd) throws IOException;

  public abstract <T> List<T> queryMulti(String cmd, RowMapper<T> mapper) throws IOException;

  public abstract int update(String cmd) throws IOException;

  protected abstract PgsqlConnection doFork();

  protected abstract void doConnect() throws IOException;

  protected abstract void doDisconnect();

  protected PgsqlConnectionStat getStat() throws IOException {
    // SELECT pg_backend_pid() 返回当前连接id
    // SELECT * FROM pg_stat_get_activity(pg_backend_pid()) 返回当前连接的视图
    String statSql = "SELECT * FROM pg_stat_get_activity(pg_backend_pid());";
    try {
      List<PgsqlConnectionStat> stats = queryMulti(statSql, new RowMapper<PgsqlConnectionStat>() {
        @Override
        public PgsqlConnectionStat mapRow(ResultSet rs, int i) throws SQLException {
          // 注意: 这里只会有1条
          PgsqlConnectionStat stat = new PgsqlConnectionStat();
          stat.setServerId(rs.getLong("datid"));
          stat.setConnectionId(rs.getLong("pid"));
          stat.setLoginId(rs.getLong("usesysid"));
          stat.setState(rs.getString("state"));
          stat.setApplication(rs.getString("application_name"));
          stat.setClientHost(rs.getString("client_addr"));
          stat.setClientPort(rs.getInt("client_port"));
          return stat;
        }
      });
      // 肯定有一条
      return stats.get(0);
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  protected void updateSettings() {
  }

  // region getter/setter

  public int getConnectTimeoutInSeconds() {
    return connectTimeoutInSeconds;
  }

  public void setConnectTimeoutInSeconds(int connectTimeoutInSeconds) {
    this.connectTimeoutInSeconds = connectTimeoutInSeconds;
  }

  public int getLoginTimeoutInSeconds() {
    return loginTimeoutInSeconds;
  }

  public void setLoginTimeoutInSeconds(int loginTimeoutInSeconds) {
    this.loginTimeoutInSeconds = loginTimeoutInSeconds;
  }

  public int getSocketTimeoutInSeconds() {
    return socketTimeoutInSeconds;
  }

  public void setSocketTimeoutInSeconds(int socketTimeoutInSeconds) {
    this.socketTimeoutInSeconds = socketTimeoutInSeconds;
  }

  public int getReceiveBufferSize() {
    return receiveBufferSize;
  }

  public void setReceiveBufferSize(int receiveBufferSize) {
    this.receiveBufferSize = receiveBufferSize;
  }

  public int getSendBufferSize() {
    return sendBufferSize;
  }

  public void setSendBufferSize(int sendBufferSize) {
    this.sendBufferSize = sendBufferSize;
  }

  // endregion getter/setter
}
