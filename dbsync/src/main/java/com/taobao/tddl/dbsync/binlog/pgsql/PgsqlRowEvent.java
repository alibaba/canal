package com.taobao.tddl.dbsync.binlog.pgsql;

import com.taobao.tddl.dbsync.binlog.event.LogHeader;

public class PgsqlRowEvent extends PgsqlLogEvent {

  public PgsqlRowEvent(LogHeader header) {
    super(header);
  }

}
