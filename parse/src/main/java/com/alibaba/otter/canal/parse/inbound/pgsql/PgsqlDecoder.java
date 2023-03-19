package com.alibaba.otter.canal.parse.inbound.pgsql;

import com.taobao.tddl.dbsync.binlog.event.FormatDescriptionLogEvent;
import com.taobao.tddl.dbsync.binlog.event.LogHeader;
import com.taobao.tddl.dbsync.binlog.pgsql.PgsqlBeginEvent;
import com.taobao.tddl.dbsync.binlog.pgsql.PgsqlCommitEvent;
import com.taobao.tddl.dbsync.binlog.pgsql.PgsqlLogEvent;
import com.taobao.tddl.dbsync.binlog.pgsql.PgsqlRowEvent;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.commons.lang.StringUtils;

public class PgsqlDecoder {

  /**
   * Decoding an event from binlog buffer.
   *
   * @param fetcher log buffer
   * @return {@link PgsqlBeginEvent} or {@link PgsqlCommitEvent} or {@link PgsqlRowEvent}
   * @see PgsqlJdbcDecodingLogFetcher#doFetch()
   */
  public PgsqlLogEvent decode(PgsqlLogFetcher fetcher) {
    if (!fetcher.hasRemaining()) {
      return null;
    }
    /*
     * header:
     *  4,when
     *  1,type
     *  4,serverId
     *  4,eventLsn
     */
    LogHeader header = new LogHeader(fetcher, new FormatDescriptionLogEvent(1));
    /*
     * body:
     *  8,lsn
     *  4,len
     *  m,destination
     *  n,eventLsn
     */
    long lsn = readLsn(fetcher);
    String destination = readDestination(fetcher);
    int len = fetcher.limit() - fetcher.position();
    byte[] data = fetcher.getData(fetcher.position(), len);
    String s = new String(data);
    PgsqlLogEvent logEvent;
    if (StringUtils.startsWith(s, "BEGIN")) {
      logEvent = new PgsqlBeginEvent(header);
    } else if (StringUtils.startsWith(s, "COMMIT")) {
      logEvent = new PgsqlCommitEvent(header);
    } else {
      logEvent = new PgsqlRowEvent(header);
    }
    logEvent.setDestination(destination);
    logEvent.setLsn(lsn);
    logEvent.setData(s);
    return logEvent;
  }

  private String readDestination(PgsqlLogFetcher fetcher) {
    int len = 4;
    byte[] lenBytes = fetcher.getData(fetcher.position(), len);
    fetcher.position(fetcher.position() + len);

    ByteBuffer buf = ByteBuffer.allocate(len);
    buf.put(lenBytes);
    buf.flip();
    int destinationLen = buf.getInt();
    byte[] bytes = fetcher.getData(fetcher.position(), destinationLen);
    fetcher.position(fetcher.position() + destinationLen);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  private long readLsn(PgsqlLogFetcher fetcher) {
    int len = 8;
    byte[] lsnBytes = fetcher.getData(fetcher.position(), len);
    fetcher.position(fetcher.position() + len);
    ByteBuffer buf = ByteBuffer.allocate(len);
    buf.put(lsnBytes);
    buf.flip();
    return buf.getLong();
  }

}
