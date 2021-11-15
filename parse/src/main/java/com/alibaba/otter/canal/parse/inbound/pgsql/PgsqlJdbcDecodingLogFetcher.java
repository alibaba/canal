package com.alibaba.otter.canal.parse.inbound.pgsql;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;

public class PgsqlJdbcDecodingLogFetcher extends PgsqlJdbcLogFetcher {

  private PGReplicationStream stream;
  private byte[] logfileName;

  @Override
  protected void start0(Long pos) throws SQLException {
    PGConnection conn = this.jdbcConnection.unwrap(PGConnection.class);
    ChainedLogicalStreamBuilder builder = conn.getReplicationAPI()
        .replicationStream()
        .logical()
        .withSlotName(destination)
        .withStatusInterval(2, TimeUnit.SECONDS)
        .withSlotOption("include-xids", "on")
        .withSlotOption("only-local", "on")
        .withSlotOption("include-timestamp", "on");
    if (pos != null && pos > 0) {
      builder.withStartPosition(LogSequenceNumber.valueOf(pos));
    }
    this.logfileName = destination.getBytes(StandardCharsets.UTF_8);
    this.stream = builder.start();
  }

  @Override
  protected boolean doFetch() throws SQLException {
    // 可参考这里的解释: https://github.com/alibaba/canal/issues/1707
    // canal拉取数据并存储在内存中,当ack之后才会将数据持久化到zk(这里记录position),所以如果有报错,可能有重复数据
    // pgsql实现: 将下面的数据写入到fetcher的buffer中,decode时需要参考fetcher的put操作
    reset();
    flush();
    ByteBuffer msg = this.stream.readPending();
    if (msg == null) {
      return true;
    }
    int offset = msg.arrayOffset();
    byte[] source = msg.array();
    int length = source.length - offset;
    long lsn = this.stream.getLastReceiveLSN().asLong();
    fetch0(lsn, source, offset, length);
    return true;
  }

  @Override
  protected void doClose() throws SQLException {
    this.stream.close();
  }

  private void flush() {
    Long lsn;
    synchronized (queue) {
      lsn = queue.poll();
      if (lsn == null) {
        return;
      }
    }
    LogSequenceNumber logSequenceNumber = LogSequenceNumber.valueOf(lsn);
    this.stream.setAppliedLSN(logSequenceNumber);
    this.stream.setFlushedLSN(logSequenceNumber);
  }

  private void reset() {
    this.position = 0;
    this.limit = 0;
  }

  private void fetch0(long lsn, byte[] msg, int offset, int len) {
    int total = 4 + 1 + 4 + 4 + 8 + 4 + logfileName.length + len;
    ensureCapacity(total);
    ByteBuffer buf = ByteBuffer.allocate(total);
    /*
    header:
     4,when
     1,type
     4,serverId
     4,eventLsn
     */
    buf.putInt(0);
    buf.put((byte) 0);
    buf.putInt((int) pgsqlConnection.queryServerId());
    buf.putInt(0);
    /*
    body:
    8,lsn
    4,logfileName len
    m, logfileName
    n,msg
     */
    buf.putLong(lsn);
    buf.putInt(logfileName.length);
    buf.put(logfileName);
    buf.put(ByteBuffer.wrap(msg, offset, len));
    buf.flip();
    buf.get(this.buffer, 0, total);
    buf.clear();

    this.position = 0;
    this.limit = total;
  }
}
