package com.taobao.tddl.dbsync.binlog.pgsql;

import com.taobao.tddl.dbsync.binlog.LogEvent;
import com.taobao.tddl.dbsync.binlog.event.LogHeader;
import org.postgresql.replication.LogSequenceNumber;

public abstract class PgsqlLogEvent extends LogEvent {

  private String destination;
  private long lsn;
  private String data;

  public PgsqlLogEvent(LogHeader header) {
    /*
    +------------------+-------------------------------------------------------+
    | Fields           |  Implementations                                      |
    +------------------+-------------------------------------------------------+
    | 1. eventLen      |                                                       |
    +------------------+-------------------------------------------------------+
    | 2. logPos        | SELECT * FROM pg_walfile_name_offset('${eventLen}');  |
    +------------------+ 1. file_name                                          |
    | 3. logFileName   | 2. file_offset                                        |
    +------------------+-------------------------------------------------------+
    | 4. when          | commit time                                           |
    +------------------+-------------------------------------------------------+
    | 5. serverId      | SELECT * FROM pg_stat_get_activity(pg_backend_pid()); |
    +------------------+-------------------------------------------------------+
    | 6. type          | B/C/I/U/D                                             |
    +------------------+-------------------------------------------------------+
    | 7. flags         | 0                                                     |
    +------------------+-------------------------------------------------------+
    | 8. checksumAlg   | 0                                                     |
    +------------------+-------------------------------------------------------+
    | 9. crc           | 0                                                     |
    +------------------+-------------------------------------------------------+
    */
    super(header);
    semival = 0;
  }

  @Override
  public final void setSemival(int semiVal) {
  }

  @Override
  public String toString() {
    return destination + " " + LogSequenceNumber.valueOf(lsn).asString() + " " + data;
  }

  public String getDestination() {
    return destination;
  }

  public void setDestination(String destination) {
    this.destination = destination;
  }

  public long getLsn() {
    return lsn;
  }

  public void setLsn(long lsn) {
    this.lsn = lsn;
  }

  public String getData() {
    return data;
  }

  public void setData(String data) {
    this.data = data;
  }
}
