package com.alibaba.otter.canal.parse.inbound.pgsql;


/**
 * <a href="https://www.postgresql.org/docs/12/monitoring-stats.html">pg_stat_activity</a>
 */
public class PgsqlConnectionStat {

  private long serverId;      // OID of the database this backend is connected to
  private long connectionId;  // Process ID of this backend
  private long loginId;       // OID of the user logged into this backend
  private String state;       /* Current overall state of this backend. Possible values are:
                              //    active,
                              //    idle,
                              //    idle in transaction,
                              //    idle in transaction (aborted),
                              //    fastpath function call,
                              //    disabled */
  private String application; // Name of the application that is connected to this backend
  private String clientHost;  // client address
  private int clientPort;     // client port

  public PgsqlConnectionStat() {
    serverId = -1;
    connectionId = -1;
    loginId = -1;
    state = "";
    application = "";
    clientHost = "";
    clientPort = -1;
  }

  public long getServerId() {
    return serverId;
  }

  public void setServerId(long serverId) {
    this.serverId = serverId;
  }

  public long getConnectionId() {
    return connectionId;
  }

  public void setConnectionId(long connectionId) {
    this.connectionId = connectionId;
  }

  public long getLoginId() {
    return loginId;
  }

  public void setLoginId(long loginId) {
    this.loginId = loginId;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public String getApplication() {
    return application;
  }

  public void setApplication(String application) {
    this.application = application;
  }

  public String getClientHost() {
    return clientHost;
  }

  public void setClientHost(String clientHost) {
    this.clientHost = clientHost;
  }

  public int getClientPort() {
    return clientPort;
  }

  public void setClientPort(int clientPort) {
    this.clientPort = clientPort;
  }
}
