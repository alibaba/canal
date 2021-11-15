package com.alibaba.otter.canal.parse.driver.pgsql;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang.StringUtils;
import org.springframework.jdbc.core.RowMapper;

/**
 * TODO: postgresql 协议实现
 */
public abstract class PgsqlConnector {

  protected final InetSocketAddress address;
  protected final String database;
  protected final String username;
  protected final String password;
  protected final AtomicBoolean connected = new AtomicBoolean(false);

  protected int connectTimeoutInSeconds = 60;
  protected int socketTimeoutInSeconds = 60;
  protected int loginTimeoutInSeconds = 60;
  protected int sendBufferSize = 1024 * 1024 * 32;    // 32MB
  protected int receiveBufferSize = 1024 * 1024 * 32; // 32MB

  public PgsqlConnector(String host, int port, String database, String username, String password) {
    this(new InetSocketAddress(host, port), database, username, password);
  }

  /**
   * @param address  where to be connected
   * @param database the database to be connected
   * @param username the user connected by
   * @param password the password connected by
   */
  public PgsqlConnector(InetSocketAddress address, String database, String username, String password) {
    checkArgument(address != null, "address is null");
    checkArgument(StringUtils.isNotBlank(database), "database is null");
    checkArgument(StringUtils.isNotBlank(username), "username is null");
    checkArgument(StringUtils.isNotBlank(password), "password is null");
    this.address = address;
    this.database = database;
    this.username = username;
    this.password = password;
  }

  public abstract void connect() throws IOException;

  public abstract void disconnect();

  public void reconnect() throws IOException {
    disconnect();
    connect();
  }

  public abstract boolean isConnected();

  public abstract <T> List<T> query(String cmd, RowMapper<T> mapper, Object... args) throws IOException;

  public abstract int update(String cmd, Object... args) throws IOException;

  // region getter/setter

  public int getConnectTimeoutInSeconds() {
    return connectTimeoutInSeconds;
  }

  public void setConnectTimeoutInSeconds(int connectTimeoutInSeconds) {
    this.connectTimeoutInSeconds = connectTimeoutInSeconds;
  }

  public int getSocketTimeoutInSeconds() {
    return socketTimeoutInSeconds;
  }

  public void setSocketTimeoutInSeconds(int socketTimeoutInSeconds) {
    this.socketTimeoutInSeconds = socketTimeoutInSeconds;
  }

  public int getLoginTimeoutInSeconds() {
    return loginTimeoutInSeconds;
  }

  public void setLoginTimeoutInSeconds(int loginTimeoutInSeconds) {
    this.loginTimeoutInSeconds = loginTimeoutInSeconds;
  }

  public int getSendBufferSize() {
    return sendBufferSize;
  }

  public void setSendBufferSize(int sendBufferSize) {
    this.sendBufferSize = sendBufferSize;
  }

  public int getReceiveBufferSize() {
    return receiveBufferSize;
  }

  public void setReceiveBufferSize(int receiveBufferSize) {
    this.receiveBufferSize = receiveBufferSize;
  }

  // endregion getter/setter

}
