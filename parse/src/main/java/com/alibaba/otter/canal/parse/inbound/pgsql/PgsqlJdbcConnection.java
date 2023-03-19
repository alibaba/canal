package com.alibaba.otter.canal.parse.inbound.pgsql;

import com.alibaba.otter.canal.parse.driver.pgsql.PgsqlJdbcConnector;
import com.alibaba.otter.canal.parse.inbound.MultiStageCoprocessor;
import com.alibaba.otter.canal.parse.inbound.SinkFunction;
import com.taobao.tddl.dbsync.binlog.pgsql.PgsqlLogEvent;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.RowMapper;

public class PgsqlJdbcConnection extends PgsqlConnection {

  static {
    try {
      Class.forName("org.postgresql.Driver");
    } catch (Throwable t) {
      throw new UnsupportedOperationException(t);
    }
  }

  public PgsqlJdbcConnection(String host, int port, String database, String username, String password) {
    this(new InetSocketAddress(host, port), database, username, password);
  }

  public PgsqlJdbcConnection(InetSocketAddress address, String database, String username, String password) {
    super(address, database, username, password);
    this.connector = new PgsqlJdbcConnector(address.getHostName(), address.getPort(), database, username, password);
  }

  @Override
  protected void doConnect() throws IOException {
    this.connector.setConnectTimeoutInSeconds(connectTimeoutInSeconds);
    this.connector.setLoginTimeoutInSeconds(loginTimeoutInSeconds);
    this.connector.setSocketTimeoutInSeconds(socketTimeoutInSeconds);
    this.connector.setSendBufferSize(sendBufferSize);
    this.connector.setReceiveBufferSize(receiveBufferSize);
    this.connector.connect();
  }

  @Override
  protected void doDisconnect() {
    this.connector.disconnect();
  }

  @Override
  protected PgsqlConnection doFork() {
    return new PgsqlJdbcConnection(this.authInfo.getAddress().getHostName(), this.authInfo.getAddress().getPort(),
        this.authInfo.getDefaultDatabaseName(), this.authInfo.getUsername(), this.authInfo.getPassword());
  }

  @Override
  public void seek(String binlog, Long binlogPosition, String gtid, SinkFunction func) throws IOException {
    throw new UnsupportedOperationException("unsupported seek(String binlog, Long binlogPosition, String gtid, SinkFunction func)");
  }

  @Override
  public void dump(String slot, Long binlogPosition, SinkFunction func) throws IOException {
    updateSettings();
    SinkFunction<PgsqlLogEvent> sf = func;
    try (PgsqlJdbcDecodingLogFetcher fetcher = new PgsqlJdbcDecodingLogFetcher()) {
      PgsqlDecoder decoder = new PgsqlDecoder();
      fetcher.start(slot, binlogPosition, this);
      while (fetcher.fetch()) {
        PgsqlLogEvent event = decoder.decode(fetcher);
        if (event == null) {
          // pgsql返回值可能为空
          {
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              break;
            }
          }
          continue;
        }
        if (!sf.sink(event)) {
          break;
        }
        fetcher.ack(event.getLsn());
      }
    } catch (IOException e) {
      throw e;
    } catch (Throwable e) {
      throw new IOException(e);
    }
  }

  @Override
  public void dump(long timestamp, SinkFunction func) throws IOException {
    throw new UnsupportedOperationException("unsupported dump(long timestamp, SinkFunction func) now");
  }

  @Override
  public void dump(String binlog, Long binlogPosition, MultiStageCoprocessor coprocessor) throws IOException {
    throw new UnsupportedOperationException("unsupported dump(String binlog, Long binlogPosition, MultiStageCoprocessor coprocessor) now");
  }

  @Override
  public void dump(long timestamp, MultiStageCoprocessor coprocessor) throws IOException {
    throw new UnsupportedOperationException("unsupported dump(long timestamp, MultiStageCoprocessor coprocessor) now");
  }

  @Override
  public boolean isConnected() {
    return this.connector.isConnected();
  }

  @Override
  public List<Map<String, Object>> query(String cmd) throws IOException {
    return queryMulti(cmd, (rs, i) -> {
      ResultSetMetaData rsmd = rs.getMetaData();
      int columnCount = rsmd.getColumnCount();
      Map<String, Object> r = new HashMap<>();
      for (int j = 1; j <= columnCount; j++) {
        String columnName = rsmd.getColumnName(j);
        r.put(columnName, rs.getObject(columnName));
      }
      return r;
    });
  }

  @Override
  public <T> List<T> queryMulti(String cmd, RowMapper<T> mapper) throws IOException {
    return this.connector.query(cmd, mapper);
  }

  @Override
  public int update(String cmd) throws IOException {
    return this.connector.update(cmd);
  }

  @Override
  public String toString() {
    // user@host:port@${serverId} [${clientId} ${state}]
    return "JdbcConnection{"
        + authInfo.getUsername() + "@" + authInfo.getAddress()
        + (stat == null ? "@-1 [-1 null]" :
        ("@" + stat.getServerId() + " [" + stat.getConnectionId() + stat.getState() + "]"));
  }

}
