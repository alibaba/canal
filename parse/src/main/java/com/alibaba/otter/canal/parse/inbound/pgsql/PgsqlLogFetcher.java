package com.alibaba.otter.canal.parse.inbound.pgsql;

import com.taobao.tddl.dbsync.binlog.LogFetcher;
import java.io.IOException;
import java.sql.SQLException;
import java.util.PriorityQueue;

public abstract class PgsqlLogFetcher extends LogFetcher {

  protected final PriorityQueue<Long> queue = new PriorityQueue<>();
  protected String destination;
  protected PgsqlConnection pgsqlConnection;
  protected volatile boolean started;

  public synchronized final void start(String destination, Long pos, PgsqlConnection connection) throws IOException {
    if (this.started) {
      return;
    }
    this.started = true;
    this.destination = destination;
    this.pgsqlConnection = connection;
    try {
      doStart(pos);
    } catch (SQLException t) {
      throw new IOException(t);
    }
  }

  @Override
  public final boolean fetch() throws IOException {
    checkStarted();
    try {
      return doFetch();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt(); // 清除中断标记
      throw new IOException(e);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public synchronized final void close() {
    if (!started) {
      return;
    }
    this.started = false;
    try {
      doClose();
    } catch (Exception ignore) {
    }
  }

  public void ack(long lsn) {
    synchronized (queue) {
      queue.add(lsn);
    }
  }

  protected void checkStarted() throws IOException {
    if (!started) {
      throw new IOException("connection has been closed");
    }
  }

  protected void doStart(Long pos) throws SQLException {
  }

  protected abstract boolean doFetch() throws Exception;

  protected abstract void doClose() throws SQLException;
}
